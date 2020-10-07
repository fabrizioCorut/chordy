%%%-------------------------------------------------------------------
%%% @author fabi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Oct 2020 16:02
%%%-------------------------------------------------------------------
-module(node4).
-author("fabi").

% Interval used to fire stabilization messages that trigger the start of the stabilization algorithm.
-define(Stabilize, 1000).

% Amount of microseconds that we should wait for the ring joining message.
-define(Timeout, 1000).

% `true` if we should print to console every step.
-define(DEBUG, false).

%% API
-export([start/1, start/2]).

%%----------------------------------------------------------------------
%% Function: start/1
%% Purpose: Starts a Chord-based Node capable of storing key-values pairs.
%% Args:    Id - Should uniquely identify the process. Used for debugging purposes.
%% Returns: ok.
%%----------------------------------------------------------------------
start(Id) ->
  start(Id, nil).

%%----------------------------------------------------------------------
%% Function: start/2
%% Purpose: Starts a Chord-based Node capable of storing key-values pairs and makes it join a pre-existing Chord ring.
%% Args:    Id - Should uniquely identify the process. Used for debugging purposes.
%%          Peer - Representative and part of the Chord ring. If nil, this is the first node in the ring.
%% Returns: ok.
%%----------------------------------------------------------------------
start(Id, Peer) ->
  timer:start(),
  spawn(
    fun() ->
      init(Id, Peer)
    end
  ).

%%----------------------------------------------------------------------
%% Function: init/2
%% Purpose: Initializes the Chord-based node and tries to connect it to the ring, if we are not the first one.
%%          If we are the first node in the ring, Successor will be set to self  and Predecessor to nil.
%% Args:    Id - Should uniquely identify the process. Used for debugging purposes.
%%          Peer - Representative and part of the Chord ring. If nil, this is the first node in the ring.
%% Returns: ok.
%%----------------------------------------------------------------------
init(Id, Peer) ->
  Predecessor = nil,
  % Try to join the ring. If Peer == nil, then Successor will be self.
  {ok, Successor} = connect(Id, Peer),
  % Schedule the stabilization to happen between a certain interval of time.
  schedule_stabilize(),
  % Start the work.
  node(Id, Predecessor, Successor, nil, monitor:create(), storage:create(), storage:create()).

%%----------------------------------------------------------------------
%% Function: connect/2
%% Purpose: Tries to connect to a Chord ring.
%% Args:    Id - Should uniquely identify the process. Used for debugging purposes.
%%          Peer - Representative and part of the Chord ring. If nil, we'll "connect" with ourselves and return self.
%% Returns: ok.
%%----------------------------------------------------------------------
connect(Id, nil) ->
  % We're starting the ring.
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
    % Did not receive a reply yet.
    debug_print("Timeout: no response.~n")
  end.

%%----------------------------------------------------------------------
%% Function: node/4
%% Purpose: Starts a Chord-based Node capable of storing key-values pairs.
%% Args:    Id - Should uniquely identify the process. Used for debugging purposes.
%%          Predecessor - The Node that preceedes us in the Chord ring.
%%          Successor - The Node that follows us in the Chord ring.
%%          Next - Our Successor's Successor.
%%          Monitor - Holds references to monitored processes.
%%          Storage - Holds the key-value pairs that this Node is managing.
%%          ReplicaStorage - Holds the key-value pairs that we have replicated from our Predecessor.
%% Returns: ok.
%%----------------------------------------------------------------------
node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage) ->
  % Start reacting to incoming messages.
  receive
    % A new Node wants to connect to the Chord ring that we're part of.
    {key, UniqueReference, Peer} ->
      debug_print("[~p]: key: request key from: ~p.~n", [self(), Peer]),
      % Return our Id, so he can use it later on to determine his relative position to us.
      Peer ! {UniqueReference, Id},
      % Recurse.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage);

    % Timer fired. Begin the stabilization algorithm by asking our Successor what his Predecessor is.
    % STABILIZE part 1.
    stabilize ->
      debug_print("[~p]: periodic stabilize.~n", [self()]),
      % Send him a request for his Predecessor.
      stabilize(Successor),
      % Recurse.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage);

    % A Node needs to know our Predecessor.
    % STABILIZE part 2.
    {request, Peer} ->
      debug_print("[~p]: request: received from: ~p.~n", [self(), Peer]),
      % Inform the Node what our Predecessor is and wait for new information.
      request(Peer, Predecessor, Successor),
      % Recurse.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage);

    % Our Successor informs us about the Predecessor that he's aware of.
    % STABILIZE part 3.
    {status, SuccessorPredecessor, SuccessorSuccessor} ->
      debug_print("[~p]: status: received SuccessorPredecessor as: ~p.~n", [self(), SuccessorPredecessor]),
      % Determine, based on his Predecessor, whether he should still be our Successor or not.
      {NewSuccessor, NextSuccessor} = stabilize(SuccessorPredecessor, SuccessorSuccessor, Id, Successor),
      debug_print("[~p]: status: decided on NewSuccessor: ~p.~n", [self(), NewSuccessor]),
      % Monitor our new Successor so we're aware of when he has crashed. Also, remove the monitoring
      % for our old Successor.
      UpdatedMonitor = monitor:remove(Successor, Monitor),
      NewMonitor = monitor:add(NewSuccessor, UpdatedMonitor),
      % Recurse with the updated Successor.
      node(Id, Predecessor, NewSuccessor, NextSuccessor, NewMonitor, Storage, ReplicaStorage);

    % A new node informs us of its existence as our Predecessor.
    % STABILIZE part 4.
    {notify, PossiblePredecessor} ->
      debug_print("[~p]: notify: received: ~p.~n", [self(), PossiblePredecessor]),
      % Determine whether he is actually our predecessor or not.
      {NewPredecessor, NewStorage, NewReplica} = notify(PossiblePredecessor, Id, Predecessor, Storage, ReplicaStorage),
      debug_print("[~p]: notify: decided on new predecessor: ~p.~n", [self(), NewPredecessor]),
      % Monitor our new Predecessor so we're aware of when he has crashed. Also, remove the monitoring
      % for our old Predecessor.
      UpdatedMonitor = monitor:remove(Predecessor, Monitor),
      NewMonitor = monitor:add(NewPredecessor, UpdatedMonitor),
      % Recurse with the updated Predecessor.
      node(Id, NewPredecessor, Successor, Next, NewMonitor, NewStorage, NewReplica);

    %% ------ Failure Detection ------ %%

    % One of the Nodes we're monitoring has crashed.
    {'DOWN', Ref, process, _Object, _Reason} ->
      case monitor:isValueOfNodeEqual(Ref, Successor, Monitor) of
        % Our Successor has died.
        true ->
          % Remove it from the monitoring and add the next process to the monitoring.
          UpdatedMonitor = monitor:remove(Successor, Monitor),
          NewMonitor = monitor:add(Next, UpdatedMonitor),
          % Start the stabilization algorithm
          stabilize(Next),
          % Recurse with the new values.
          node(Id, Predecessor, Next, nil, NewMonitor, Storage, ReplicaStorage);

        false ->
          % Check if our Predecessor has failed.
          case monitor:isValueOfNodeEqual(Ref, Predecessor, Monitor) of
            true ->
              % De-monitor the Predecessor process.
              NewMonitor = monitor:remove(Predecessor, Monitor),
              % Our predecessor has died, takeover the Key-Values that he was responsible for.
              MergedStore = storage:merge(ReplicaStorage, Storage),
              % There's not much that we can do, set the Predecessor as nil and leave everything as is.
              node(Id, nil, Successor, Next, NewMonitor, MergedStore, storage:create());

            false ->
              % Unknown node, do nothing.
              node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage)
          end
      end;

    %% ------ Storage related message handling ------ %%

    % We're requested to add a new Value to the Key.
    {add, Key, Value, Qref, Client} ->
      % Determine whether we should be forwarding the message or add it to our Storage.
      Added = add(Key, Value, Qref, Client, Id, Predecessor, Successor, Storage),
      % Recurse with the updated Storage.
      node(Id, Predecessor, Successor, Next, Monitor, Added, ReplicaStorage);

    % We're requested to lookup a certain Value found at a certain Key.
    {lookup, Key, Qref, Client} ->
      % Determine whether we should be forwarding the message or we're managing the Key-Value pair, case in which
      % inform the Client of the Value.
      lookup(Key, Qref, Client, Id, Predecessor, Successor, Storage),
      % Recurse.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage);

    % We're being handed over Values of our Successor.
    {handover, Elements, Replica} ->
      % Merge the values received with what we already have, overwriting any existing values/
      MergedStorage = storage:merge(Elements, Storage),
      MergedReplica = storage:merge(Replica, ReplicaStorage),
      % Recurse with the new storage.
      node(Id, Predecessor, Successor, Next, Monitor, MergedStorage, MergedReplica);

    {replicate, Key, Value} ->
      % Add the Key-Value pair nevertheless.
      NewReplica = storage:add(Key, Value, ReplicaStorage),
      % Recurse with the new storage capabilities.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, NewReplica);

    %%% ------ DEBUG messages ------ %%

    % Start going through the ring and see how much it takes us and the Nodes that we go through.
    probe ->
      debug_print("[~p]: probe: received request.~n", [self()]),
      create_probe(Id, Successor),
      % Recurse.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage);

    % We've received back the message, so the ring is complete. Print the results.
    {probe, Id, Nodes, Time} ->
      debug_print("[~p]: probe: finished request.~n", [self()]),
      remove_probe(Time, Nodes),
      % Recurse.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage);

    % We've received a probe that has started from another Node. Just forward it.
    {probe, Ref, Nodes, Time} ->
      debug_print("[~p]: probe: forwarding request.~n", [self()]),
      forward_probe(Ref, Nodes, Time, Id, Successor),
      % Recurse.
      node(Id, Predecessor, Successor, Next, Monitor, Storage, ReplicaStorage)
  end.

%% ------ Storage ------ %%

%%----------------------------------------------------------------------
%% Function: add/8
%% Purpose: Determines whether we should handle the Key-Value pair or we should forward it to our Successor.
%%          If we are to handle it, we update our Storage and return it.
%% Args:    Key - Key under which the Value should be put.
%%          Value - The actual Value that we should store.
%%          Qref - Unique identifier used to map the request to the response.
%%          Client - Pid of the process that requested us to add the Key-Value pair..
%%          Id - Should uniquely identify the process. Used for debugging purposes.
%%          Predecessor - The Node that precedes us in the Chord ring.
%%          Successor - The Node that follows us in the Chord ring.
%%          Storage - Holds the key-value pairs that this Node is managing.
%% Returns: Updated Storage containing the Key-Value pair..
%%----------------------------------------------------------------------
add(Key, Value, Qref, Client, Id, {PredecessorKey, _}, {_, SuccessorPid}, Storage) ->
  % Should we manage the Key? e.g. is the key between our Id and the one of the Predecessor?
  case key:between(Key, PredecessorKey, Id) of
    % Yes. We should store the Value.
    true ->
      % Inform the Client that we can handle his request.
      Client ! {Qref, ok},
      % Update the storage.
      SuccessorPid ! {replicate, Key, Value},
      % Inform the Successor of what Key-Value we're storing.
      storage:add(Key, Value, Storage);

    % No. Forward the request to our Successor.
    false ->
      SuccessorPid ! {add, Key, Value, Qref, Client},
      % Return the same storage.
      Storage
  end.

%%----------------------------------------------------------------------
%% Function: lookup/7
%% Purpose: Determines whether we handle the Key whose Value is requested or we should forward it to our Successor.
%%          If we are to handle it, look-up the Value that is stored under the Key in our local Storage.
%%          The value is communicated directly to the Client.
%% Args:    Key - Key whose value we're looking for.
%%          Qref - Unique identifier used to map the request to the response.
%%          Client - Pid of the process that requested us to lookup the Key-Value pair and which is expecting a response.
%%          Id - Should uniquely identify the process. Used for debugging purposes.
%%          Predecessor - The Node that precedes us in the Chord ring.
%%          Successor - The Node that follows us in the Chord ring.
%%          Storage - Holds the key-value pairs that this Node is managing.
%% Returns: ok.
%%----------------------------------------------------------------------
lookup(Key, Qref, Client, Id, {PredecessorKey, _}, {_, SuccessorPid}, Storage) ->
  % Should we manage the Key? e.g. is the key between our Id and the one of the Predecessor?
  case key:between(Key, PredecessorKey, Id) of
    % Yes.
    true ->
      % Extract the value.
      Result = storage:lookup(Key, Storage),
      % And send it to the requesting Client.
      Client ! {Qref, Result},
      ok;

    % No.
    false ->
      % Forward the request to the Successor.
      SuccessorPid ! {lookup, Key, Qref, Client},
      ok
  end.

%% ------ Stabilize ------ %%

%%----------------------------------------------------------------------
%% Function: stabilize/1
%% Purpose: Begins the stabilization algorithm by requesting our Successor for his Predecessor.
%% Args:    Successor - The Node that follows us in the Chord ring.
%% Returns: ok.
%%----------------------------------------------------------------------
stabilize(nil) ->
  ok;
stabilize({_, SuccessorPid}) ->
  SuccessorPid ! {request, self()},
  ok.


%%----------------------------------------------------------------------
%% Function: stabilize/4
%% Purpose: We have received the Predecessor of our Successor. Determine whether this Predecessor should actually become
%%          our new Successor or not. In either case, inform the new Successor (be it even the old one ~ unless it's this
%%          current Node as its Predecessor, which means that the connection between us and the Successor is stable)
%%          that we have him as our Successor.
%% Args:    Predecessor - Predecessor of our current Successor. The node that precedes our Successor in the Chord ring.
%%          SuccessorSuccessor - Successor of our current Successor.
%%          Id - Should uniquely identify the process. Used for debugging purposes.
%%          Successor - Our current Successor. The Node that follows us in the Chord ring.
%% Returns: A new Successor node.
%%----------------------------------------------------------------------
stabilize(Predecessor, SuccessorSuccessor, Id, {SuccessorKey, SuccessorPid} = Successor) ->
  Self = self(),
  % Determine who is this Predecessor.
  case Predecessor of
    % nil means that our Successor doesn't have a Predecessor.
    nil ->
      debug_print("[~p]: stabilize: nil predecessor.~n", [self()]),
      % Inform our Successor that we're his Predecessor.
      SuccessorPid ! {notify, {Id, Self}},
      % Return the same Successor since our connection is now stable.
      {Successor, SuccessorSuccessor};

    % Our has as its Predecessor himself. The stabilize algorithm has been performed with only himself in the ring.
    % So his Predecessor has ultimately become himself as well. Make sure that the Successor is not in fact ourselves,
    % We don't want to go into an infinite loop.
    {SuccessorKey, _} when Self /= SuccessorPid ->
      debug_print("[~p]: stabilize: self predecessor but we're not communicating with self.~n", [self()]),
      % Inform our Successor that we're his Predecessor.
      SuccessorPid ! {notify, {Id, Self}},
      % Return the same Successor since our connection is now stable.
      {Successor, SuccessorSuccessor};

    % Our has as its Predecessor himself. The stabilize algorithm has been performed with only himself in the ring.
    % So his Predecessor has ultimately become himself as well. Since we tested in the previous case that Self /= SuccessorPid
    % this means that Self == SuccessorPid, which means that we're running the algorithm with just ourselves in the ring.
    {SuccessorKey, _} ->
      debug_print("[~p]: stabilize: self predecessor.~n", [self()]),
      % Do nothing and return our current Successor.
      {Successor, SuccessorSuccessor};

    % The Successor has the current Node as his Predecessor. We have him as a Successor, then the connection between us
    % is stable.
    {Id, _} ->
      debug_print("[~p]: stabilize: correct predecessor.~n", [self()]),
      % Do nothing and return our current Successor.
      {Successor, SuccessorSuccessor};

    % Our Successor has a different Predecessor.
    {PredecessorKey, PredecessorPid} ->
      % Determine where this Predecessor is between us and our Successor.
      case key:between(PredecessorKey, Id, SuccessorKey) of
        % This Predecessor sits between us and our Successor. This means that his Predecessor
        % should be our new Successor.
        true ->
          debug_print("[~p]: stabilize: new successor: ~p.~n", [self(), PredecessorKey]),
          % Inform our new Successor that we're considering him our Successor.
          PredecessorPid ! {notify, {Id, Self}},
          % Return our new Successor
          {Predecessor, Successor};

        % His Predecessor is out of the bounds defined by us and our Successor.
        false ->
          debug_print("[~p]: stabilize: same successor: ~p.~n", [self(), PredecessorKey]),
          % Inform our Successor that we're his Predecessor.
          SuccessorPid ! {notify, {Id, Self}},
          % Return the same Successor.
          {Successor, SuccessorSuccessor}
      end
  end.

%%----------------------------------------------------------------------
%% Function: request/3
%% Purpose: Handles a stabilization request from a Peer. Will inform the Peer of our current Predecessor.
%% Args:    Peer - Pid of the Node that requested information about our Predecessor.
%%          Predecessor - The Node that precedes us in the Chord ring.
%%          Successor - The node the succeeds us in the Chord ring.
%% Returns: ok.
%%----------------------------------------------------------------------
request(Peer, Predecessor, Successor) ->
  Peer ! {status, Predecessor, Successor},
  ok.

%%----------------------------------------------------------------------
%% Function: notify/5
%% Purpose: Handles a notification from a possible Predecessor that we're being considered his Successor.
%%          If this possible Predecessor is actually our new Predecessor, we should split the Key-Value Storage with him based
%%          on its Key.
%% Args:    PossiblePredecessor - A possible Node that has set us as his Successor. We should verify that this is accurate.
%%          Id - Should uniquely identify the process. Used for debugging purposes.
%%          Predecessor - Our current Predecessor. The Node that precedes us in the Chord ring.
%%          Storage - Holds the key-value pairs that this Node is managing.
%%          ReplicatedStorage - Holds the key-value pairs that we have replicated from our Predecessor.
%% Returns: {NewPredecessor, UpdatedStorage}.
%%----------------------------------------------------------------------
notify({NewPredecessorKey, _NewPredecessorPid} = PossiblePredecessor, Id, Predecessor, Storage, ReplicatedStorage) ->
  case Predecessor of
    % We don't have a Predecessor yet.
    nil ->
      debug_print("[~p]: notify: nil Predecessor: ~p.~n", [self(), Predecessor]),
      % Inform our Predecessor that he is our Predecessor now.
      % request(NewPredecessorPid, PossiblePredecessor, Successor),
      % Handover the Key-Value storage between us and our new Predecessor based on our Keys.
      {Keep, NewReplica} = handover(Id, Storage, ReplicatedStorage, PossiblePredecessor),
      % Return this new Predecessor and the values that we're managing now.
      {PossiblePredecessor, Keep, NewReplica};

    % It's a new Predecessor.
    {PredecessorKey, _} ->
      % Determine whether he's actually between us and our current Predecessor.
      case key:between(NewPredecessorKey, PredecessorKey, Id) of
        % He is, he should in fact be our new Predecessor.
        true ->
          debug_print("[~p]: notify: new Predecessor: ~p.~n", [self(), PossiblePredecessor]),
          % Inform our Predecessor that he is our Predecessor now.
          % request(NewPredecessorPid, PossiblePredecessor, Successor),
          % Handover the Key-Value storage between us and our new Predecessor based on our Keys.
          {Keep, NewReplica} = handover(Id, Storage, ReplicatedStorage, PossiblePredecessor),
          % Return this new Predecessor and the values that we're managing now.
          {PossiblePredecessor, Keep, NewReplica};

        % He is not a new Predecessor. Do nothing.
        false ->
          debug_print("[~p]: notify: same Predecessor: ~p.~n", [self(), PossiblePredecessor]),
          % Inform the NewProcessorPid of our actual Predecessor so he can re-evaluate his algorithm.
          % request(NewPredecessorPid, Predecessor, Successor),
          % Return the same Predecessor and Storage.
          {Predecessor, Storage, ReplicatedStorage}
      end
  end.

%%----------------------------------------------------------------------
%% Function: handover/3
%% Purpose: Splits the Key-Value Storage and ReplicatedStorage based on Id and NewPredecessorKey and sends the NewPredecessor his Key-Values.
%% Args:    Id - Should uniquely identify the process. Used for debugging purposes.
%%          Storage - Holds the key-value pairs that this Node is managing.
%%          ReplicatedStorage - Holds the key-value pairs that we have replicated from our Predecessor.
%%          NewPredecessor - Our new Predecessor. The Node that preceedes us in the Chord ring.
%% Returns: Updated Storage, without the values that our NewPredecessor now manages.
%%----------------------------------------------------------------------
handover(Id, Storage, ReplicatedStorage, {NewPredecessorKey, NewPredecessorPid}) ->
  % Split the Storage between our Id and the NewPredecessor's
  {Updated, Rest} = storage:split(NewPredecessorKey, Id, Storage),
  % Send him part of the Storage that we have based on a split of its key
  % Also send him all the Replica we have since he will be the Successor of our former Predecessor.
  NewPredecessorPid ! {handover, Rest, ReplicatedStorage},
  % Return the remaining Storage that we should continue to manage.
  % Our new replica is exactly the part that we've handed over.
  {Updated, Rest}.

%%----------------------------------------------------------------------
%% Function: schedule_stabilize/0
%% Purpose: Schedules a timer that will fire with a rate of ?Stabilize microseconds, sending self() the message stabilize.
%% Returns: ok.
%%----------------------------------------------------------------------
schedule_stabilize() ->
  % Send message instead of calling a specific method with parameters so we can trace it better.
  timer:send_interval(?Stabilize, self(), stabilize),
  ok.

%% ------ DEBUG ------ %%

%%----------------------------------------------------------------------
%% Function: create_probe/2
%% Purpose: Starts sending a probe message to our Successor.
%% Args:    Id - Should uniquely identify the process. Used for debugging purposes.
%%          Successor - The node the succeeds us in the Chord ring.
%% Returns: ok.
%%----------------------------------------------------------------------
create_probe(Id, {_SuccessorKey, SuccessorPid}) ->
  % Record the start time.
  StartTime = erlang:system_time(micro_seconds),
  % Add us as the first Node and forward the message.
  SuccessorPid ! {probe, Id, [{Id, self()}], StartTime},
  ok.

%%----------------------------------------------------------------------
%% Function: remove_probe/2
%% Purpose: Ends the probing message since the probe that we started has been send back to us.
%%          Prints debug information about the ring: time it took to walk and the Nodes that we went through.
%% Args:    Time - Start time of the probing.
%%          Nodes - Nodes that the probe went through.
%% Returns: ok.
%%----------------------------------------------------------------------
remove_probe(Time, Nodes) ->
  % Calculate the time by subtracting the start time.
  RoundTripTime = erlang:system_time(micro_seconds) - Time,
  % Print the information to the console.
  io:format("RoundTrip time: ~p. Passed through: ~p nodes.~n", [RoundTripTime, length(Nodes)]).

%%----------------------------------------------------------------------
%% Function: forward_probe/5
%% Purpose: Forwards the probe to our Successor. Appends us as a Node to the list of Nodes.
%% Args:    Ref - Unique identifier of the Node that started the probe.
%%          Nodes - Nodes that the probe has went through since it has started.
%%          Time - Time, in the system of the Node that requested the probing, when the probing has started.
%%          Id - Should uniquely identify the process. Used for debugging purposes.
%%          Successor - The node the succeedes us in the Chord ring.
%% Returns: ok.
%%----------------------------------------------------------------------
forward_probe(Ref, Nodes, Time, Id, {_SuccessorKey, SuccessorPid}) ->
  % Append us to the lis of Nodes through which the probe has went.
  SuccessorPid ! {probe, Ref, [{Id, self()} | Nodes], Time},
  ok.

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