%%%-------------------------------------------------------------------
%%% @author fabi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Oct 2020 16:02
%%%-------------------------------------------------------------------
-module(node1).
-author("fabi").

% Interval used to fire stabilization messages that trigger the start of the stabilization algorithm.
-define(Stabilize, 100).

% Amount of microseconds that we should wait for the ring joining message.
-define(Timeout, 1000).

% `true` if we should print to console every step.
-define(DEBUG, true).

%% API
-export([start/1, start/2]).

% Nodes will be represented as tuples: {Key, Pid}.

start(Id) ->
  start(Id, nil).

start(Id, Peer) ->
  timer:start(),
  spawn(fun() -> init(Id, Peer) end).

init(Id, Peer) ->
  Predecessor = nil,
  {ok, Successor} = connect(Id, Peer),
  schedule_stabilize(),
  node(Id, Predecessor, Successor).

connect(Id, nil) ->
  {ok, {Id, self()}};
connect(_Id, Peer) ->
  % Use a unique reference in order to wait for the exact reply and not others.
  UniqueReference = make_ref(),
  % Request the Peer's key.
  Peer ! {key, UniqueReference, self()},
  receive
    {UniqueReference, PossibleSuccessorKey} ->
      debug_print("[~p]: connect: received key: ~p.~n", [self(), PossibleSuccessorKey]),
      % Build the Node from the key and the Peer we've been given.
      {ok, {PossibleSuccessorKey, Peer}}
  after ?Timeout ->
    debug_print("Timeout: no response.~n")
  end.

node(Id, Predecessor, Successor) ->
  receive
    % A peer needs to know our key.
    {key, UniqueReference, Peer} ->
      debug_print("[~p]: key: request key from: ~p.~n", [self(), Peer]),
      Peer ! {UniqueReference, Id},
      node(Id, Predecessor, Successor);

    % A new node informs us of its existence.
    % STABILIZE part 3.
    {notify, PossiblePredecessor} ->
      debug_print("[~p]: notify: received: ~p.~n", [self(), PossiblePredecessor]),
      NewPredecessor = notify(PossiblePredecessor, Id, Predecessor),
      debug_print("[~p]: notify: decided on new predecessor: ~p.~n", [self(), NewPredecessor]),
      node(Id, NewPredecessor, Successor);

    % A predecessor needs to know our Predecessor.
    % STABILIZE part 2.
    {request, Peer} ->
      debug_print("[~p]: request: received from: ~p.~n", [self(), Peer]),
      request(Peer, Predecessor),
      node(Id, Predecessor, Successor);

    % Timer fired. Request stabilization process from the Successor.
    % STABILIZE part 1.
    stabilize ->
      debug_print("[~p]: periodic stabilize.~n", [self()]),
      stabilize(Successor),
      node(Id, Predecessor, Successor);

    % Our successor informs us about its predecessor.
    % Result of {request, Peer}.
    {status, SuccessorPredecessor} ->
      debug_print("[~p]: status: received SuccessorPredecessor as: ~p.~n", [self(), SuccessorPredecessor]),
      NewSuccessor = stabilize(SuccessorPredecessor, Id, Successor),
      debug_print("[~p]: status: decided on NewSuccessor: ~p.~n", [self(), NewSuccessor]),
      node(Id, Predecessor, NewSuccessor);

    %%% DEBUG

    probe ->
      debug_print("[~p]: probe: received request.~n", [self()]),
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor);

    {probe, Id, Nodes, Time} ->
      debug_print("[~p]: probe: finished request.~n", [self()]),
      remove_probe(Time, Nodes),
      node(Id, Predecessor, Successor);

    {probe, Ref, Nodes, Time} ->
      debug_print("[~p]: probe: forwarding request.~n", [self()]),
      forward_probe(Ref, Nodes, Time, Id, Successor),
      node(Id, Predecessor, Successor)
  end.

create_probe(Id, {_SuccessorKey, SuccessorPid}) ->
  StartTime = erlang:system_time(micro_seconds),
  SuccessorPid ! {probe, Id, [{Id, self()}], StartTime}.

remove_probe(Time, Nodes) ->
  RoundTripTime = erlang:system_time(micro_seconds) - Time,
  debug_print("RoundTrip time: ~p. Passed through: ~p.~n", [RoundTripTime, Nodes]).

forward_probe(Ref, Nodes, Time, Id, {_SuccessorKey, SuccessorPid}) ->
  SuccessorPid ! {probe, Ref, [{Id, self()} | Nodes], Time}.

stabilize({_, SuccessorPid}) ->
  SuccessorPid ! {request, self()}.

% We have received the Predecessor of our Successor.
% See if we should inform the Predecessor of our existence or we should
% modify our Successor.
stabilize(Predecessor, Id, {SuccessorKey, SuccessorPid} = Successor) ->
  Self = self(),
  case Predecessor of
    nil ->
      debug_print("[~p]: stabilize: nil predecessor.~n", [self()]),
      SuccessorPid ! {notify, {Id, Self}},
      Successor;

    {SuccessorKey, _} when Self /= SuccessorPid ->
      debug_print("[~p]: stabilize: self predecessor but we're not communicating with self.~n", [self()]),
      SuccessorPid ! {notify, {Id, Self}},
      Successor;

    {SuccessorKey, _} ->
      debug_print("[~p]: stabilize: self predecessor.~n", [self()]),
      Successor;

    % The Successor knows about us. The ring is stable.
    {Id, _} ->
      debug_print("[~p]: stabilize: correct predecessor.~n", [self()]),
      Successor;

    {PredecessorKey, PredecessorPid} ->
      case key:between(PredecessorKey, Id, SuccessorKey) of
        true ->
          % PredecessorKey is between us and SuccessorKey. Make it our Successor.
          debug_print("[~p]: stabilize: new successor: ~p.~n", [self(), PredecessorKey]),
          PredecessorPid ! {notify, {Id, Self}},
          Predecessor;

        false ->
          % We are between the Successor's Predecessor and the Successor.
          debug_print("[~p]: stabilize: same successor: ~p.~n", [self(), PredecessorKey]),
          SuccessorPid ! {notify, {Id, Self}},
          Successor
      end
  end.

request(Peer, Predecessor) ->
  case Predecessor of
    nil ->
      Peer ! {status, nil};

    {_PredecessorKey, _PredecessorPid} ->
      Peer ! {status, Predecessor}
  end.

notify({NewPredecessorKey, NewPredecessorPid} = PossiblePredecessor, Id, Predecessor) ->
  case Predecessor of
    nil ->
      debug_print("[~p]: notify: nil Predecessor: ~p.~n", [self(), Predecessor]),
      % We do not have a Predecessor, use the one provided by us.
      request(NewPredecessorPid, PossiblePredecessor),
      PossiblePredecessor;

    {PredecessorKey, _} ->
      case key:between(NewPredecessorKey, PredecessorKey, Id) of
        true ->
          debug_print("[~p]: notify: new Predecessor: ~p.~n", [self(), PossiblePredecessor]),
          % The NewPredecessor is actually our new Predecessor.
          request(NewPredecessorPid, PossiblePredecessor),
          PossiblePredecessor;

        false ->
          % The NewPredecessor is not our predecessor, use the one we have already.
          % Inform the NewProcessorPid that this is our Predecessor, so he can re-evaluate his algorithm.
          debug_print("[~p]: notify: same Predecessor: ~p.~n", [self(), PossiblePredecessor]),
          request(NewPredecessorPid, Predecessor),
          Predecessor
      end
  end.

schedule_stabilize() ->
  % Send a stabilize message to self. Do that instead of calling a specific method so
  % we can trace it better.
  timer:send_interval(?Stabilize, self(), stabilize).

%%----------------------------------------------------------------------
%% Function: debugPrint/2
%% Purpose: Prints to the console only if we're running in debug mode.
%% Returns: ok.
%%----------------------------------------------------------------------
debug_print(Format) ->
  case ?DEBUG of
    true ->
      io:format(Format);
    false ->
      ok
  end.
debug_print(Format, Arguments) ->
  case ?DEBUG of
    true ->
      io:format(Format, Arguments);
    false ->
      ok
  end.