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

-define(Stabilize, 15000).
-define(Timeout, 1000).

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
      io:format("[~p]: connect: received key: ~p.~n", [self(), PossibleSuccessorKey]),
      % Build the Node from the key and the Peer we've been given.
      {ok, {PossibleSuccessorKey, Peer}}
  after ?Timeout ->
    io:format("Timeout: no response.~n")
  end.

node(Id, Predecessor, Successor) ->
  receive
    % A peer needs to know our key.
    {key, UniqueReference, Peer} ->
      io:format("[~p]: key: request key from: ~p.~n", [self(), Peer]),
      Peer ! {UniqueReference, Id},
      node(Id, Predecessor, Successor);

    % A new node informs us of its existence.
    % STABILIZE part 3.
    {notify, PossiblePredecessor} ->
      io:format("[~p]: notify: received: ~p.~n", [self(), PossiblePredecessor]),
      NewPredecessor = notify(PossiblePredecessor, Id, Predecessor),
      io:format("[~p]: notify: decided on new predecessor: ~p.~n", [self(), NewPredecessor]),
      node(Id, NewPredecessor, Successor);

    % A predecessor needs to know our Predecessor.
    % STABILIZE part 2.
    {request, Peer} ->
      io:format("[~p]: request: received from: ~p.~n", [self(), Peer]),
      request(Peer, Predecessor),
      node(Id, Predecessor, Successor);

    % Timer fired. Request stabilization process from the Successor.
    % STABILIZE part 1.
    stabilize ->
      io:format("[~p]: periodic stabilize.~n", [self()]),
      stabilize(Successor),
      node(Id, Predecessor, Successor);

    % Our successor informs us about its predecessor.
    % Result of {request, Peer}.
    {status, SuccessorPredecessor} ->
      io:format("[~p]: status: received SuccessorPredecessor as: ~p.~n", [self(), SuccessorPredecessor]),
      NewSuccessor = stabilize(SuccessorPredecessor, Id, Successor),
      io:format("[~p]: status: decided on NewSuccessor: ~p.~n", [self(), NewSuccessor]),
      node(Id, Predecessor, NewSuccessor);

    %%% DEBUG

    probe ->
      io:format("[~p]: probe: received request.~n", [self()]),
      create_probe(Id, Successor),
      node(Id, Predecessor, Successor);

    {probe, Id, Nodes, Time} ->
      io:format("[~p]: probe: finished request.~n", [self()]),
      remove_probe(Time, Nodes),
      node(Id, Predecessor, Successor);

    {probe, Ref, Nodes, Time} ->
      io:format("[~p]: probe: forwarding request.~n", [self()]),
      forward_probe(Ref, Nodes, Time, Id, Successor),
      node(Id, Predecessor, Successor)
  end.

create_probe(Id, {_SuccessorKey, SuccessorPid}) ->
  StartTime = erlang:system_time(micro_seconds),
  SuccessorPid ! {probe, Id, [{Id, self()}], StartTime}.

remove_probe(Time, Nodes) ->
  RoundTripTime = erlang:system_time(micro_seconds) - Time,
  io:format("RoundTrip time: ~p. Passed through: ~p.~n", [RoundTripTime, Nodes]).

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
      io:format("[~p]: stabilize: nil predecessor.~n", [self()]),
      SuccessorPid ! {notify, {Id, Self}},
      Successor;

    {SuccessorKey, _} when Self /= SuccessorPid ->
      io:format("[~p]: stabilize: self predecessor but we're not communicating with self.~n", [self()]),
      SuccessorPid ! {notify, {Id, Self}},
      Successor;

    {SuccessorKey, _} ->
      io:format("[~p]: stabilize: self predecessor.~n", [self()]),
      Successor;

    % The Successor knows about us. The ring is stable.
    {Id, _} ->
      io:format("[~p]: stabilize: correct predecessor.~n", [self()]),
      Successor;

    {PredecessorKey, PredecessorPid} ->
      case key:between(PredecessorKey, Id, SuccessorKey) of
        true ->
          % PredecessorKey is between us and SuccessorKey. Make it our Successor.
          io:format("[~p]: stabilize: new successor: ~p.~n", [self(), PredecessorKey]),
          PredecessorPid ! {notify, {Id, Self}},
          Predecessor;

        false ->
          % We are between the Successor's Predecessor and the Successor.
          io:format("[~p]: stabilize: same successor: ~p.~n", [self(), PredecessorKey]),
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
      io:format("[~p]: notify: nil Predecessor: ~p.~n", [self(), Predecessor]),
      % We do not have a Predecessor, use the one provided by us.
      request(NewPredecessorPid, PossiblePredecessor),
      PossiblePredecessor;

    {PredecessorKey, _} ->
      case key:between(NewPredecessorKey, PredecessorKey, Id) of
        true ->
          io:format("[~p]: notify: new Predecessor: ~p.~n", [self(), PossiblePredecessor]),
          % The NewPredecessor is actually our new Predecessor.
          request(NewPredecessorPid, PossiblePredecessor),
          PossiblePredecessor;

        false ->
          % The NewPredecessor is not our predecessor, use the one we have already.
          % Inform the NewProcessorPid that this is our Predecessor, so he can re-evaluate his algorithm.
          io:format("[~p]: notify: same Predecessor: ~p.~n", [self(), PossiblePredecessor]),
          request(NewPredecessorPid, Predecessor),
          Predecessor
      end
  end.

schedule_stabilize() ->
  % Send a stabilize message to self. Do that instead of calling a specific method so
  % we can trace it better.
  timer:send_interval(?Stabilize, self(), stabilize).