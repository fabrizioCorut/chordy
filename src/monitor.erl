%%%-------------------------------------------------------------------
%%% @author fabi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% Used o map Nodes with their unique monitoring reference.
%%% Uses the BIF erlang:monitor/2 for monitoring a process using a PID.
%%% @end
%%% Created : 06. Oct 2020 18:41
%%%-------------------------------------------------------------------
-module(monitor).
-author("fabi").

%% API
-export([create/0, add/2, remove/2, isValueOfNodeEqual/3]).

%%----------------------------------------------------------------------
%% Function: create/0
%% Purpose: Creates a new Monitor.
%% Returns: ok.
%%----------------------------------------------------------------------
create() ->
  dict:new().

%%----------------------------------------------------------------------
%% Function: add/2
%% Purpose: Starts monitoring the Node and maps the Node's unique identifier to the
%%          monitoring reference returned by the erlang:monitor/2 BIF.
%% Args:    Node - Node that we should be monitoring.
%%          Monitor - Current Monitor.
%% Returns: Updated Monitor containing the record of the Node and its monitoring reference.
%%----------------------------------------------------------------------
add(nil, Monitor) ->
  Monitor;
add({NodeKey, NodePid}, Monitor) ->
  Ref = erlang:monitor(process, NodePid),
  dict:store(NodeKey, Ref, Monitor).

%%----------------------------------------------------------------------
%% Function: remove/2
%% Purpose: Removes the record that we have of the Node and de-monitors.
%% Args:    Node - Node that we were monitoring.
%%          Monitor - Current Monitor.
%% Returns: Updated monitor that doesn't contain the record of Node anymore.
%%----------------------------------------------------------------------
remove(nil, Monitor) ->
  Monitor;
remove({NodeKey, _NodePid}, Monitor) ->
  case dict:find(NodeKey, Monitor) of
    {ok, Ref} ->
      erlang:demonitor(Ref, [flush]),
      dict:erase(NodeKey, Monitor);

    _ ->
      Monitor
  end.

%%----------------------------------------------------------------------
%% Function: isValueOfNodeEqual/3
%% Purpose: Checks whether the value stored for a certain Node is equal with the provided Ref.
%% Args:    Ref - Unique identifier of the reference created between the current process and the one represented by the Node.
%%          Node - Node that we want to check the Ref against.
%%          Monitor - Current Monitor.
%% Returns: true/ false.
%%----------------------------------------------------------------------
isValueOfNodeEqual(Ref, {NodeKey, _NodePid}, Monitor) ->
  % Search the value of the Node.
  case dict:find(NodeKey, Monitor) of
    % The stored value is the same as the Ref.
    {ok, Ref} ->
      true;

    % Different value, return false.
    _Other ->
      false
  end.

