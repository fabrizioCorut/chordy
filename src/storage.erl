%%%-------------------------------------------------------------------
%%% @author fabi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Oct 2020 11:10
%%%-------------------------------------------------------------------
-module(storage).
-author("fabi").

%% API
-export([create/0, add/3, lookup/2, split/3, merge/2]).

%%----------------------------------------------------------------------
%% Function: create/0
%% Purpose: Creates an empty Storage.
%%----------------------------------------------------------------------
create() ->
  [].

%%----------------------------------------------------------------------
%% Function: add/3
%% Purpose: Inserts the Key-Value pair into the Storage. If it already exists, it will be overwritten.
%% Args:    Key - Unique identifier under which we should keep the Value.
%%          Value - Representation of a value that is to be stored.
%%          Storage - Current Storage.
%% Returns: Updated Storage containing the new Key-Value pair..
%%----------------------------------------------------------------------
add(_Key, _Value, []) ->
  [];
add(Key, Value, [{HeadKey, _HeadValue} = Head | T]) when Key < HeadKey ->
  % Change the key to inf so Key < HeadKey will never be satisfied.
  [{Key, Value}, Head | add(inf, Value, T)];
add(Key, Value, [{HeadKey, _HeadValue} | T]) when Key == HeadKey ->
  % Replace the existing.
  [{Key, Value} | add(inf, Value, T)];
add(Key, Value, [H | T]) ->
  [H | add(Key, Value, T)].

%%----------------------------------------------------------------------
%% Function: lookup/2
%% Purpose: Looks-up a value that is stored under a unique Key.
%% Args:    Key - Unique identifier under which the Value should be stored.
%%          Storage - Current Storage.
%% Returns: The Value from the Key-Value pair stored or false if it could not be found.
%%----------------------------------------------------------------------
lookup(_, []) ->
  false;
lookup(Key, [{Key, _Value} = Head | _T]) ->
  Head;
lookup(Key, [_ | T]) ->
  lookup(Key, T).

%%----------------------------------------------------------------------
%% Function: split/3
%% Purpose: Splits the Storage into two. Be F the interval determined by the lowest key and the highest key.
%%          Each of the resulted Storages will contain Key-Value pairs from the intervals: [From, To] and F - [From, To].
%% Args:    From - Key starting with which we should start the split.
%%          To - Key up to which we should consider the split.
%%          Storage - Current Storage.
%% Returns: {Updated, Rest}. Updated will contain Key-Value pairs from the interval determined by From and To.
%%----------------------------------------------------------------------
split(From, To, Store) ->
  {FinalUpdated, FinalRest} = lists:foldl(
    fun({Key, Value}, Acc) ->
      {Updated, Rest} = Acc,
      if
        Key < From ->
          NewRest = [{Key, Value} | Rest],
          {Updated, NewRest};

        Key > To ->
          NewRest = [{Key, Value} | Rest],
          {Updated, NewRest};

        true ->
          NewUpdated = [{Key, Value} | Updated],
          {NewUpdated, Rest}
      end
    end,
    {[], []},
    Store
  ),
  % Sort back the {Key, Value} arrays since they might be used back with this method.
  {lists:sort(fun store_sort/2, FinalUpdated), lists:sort(fun store_sort/2, FinalRest)}.

%%----------------------------------------------------------------------
%% Function: store_sort/2
%% Purpose: Storage Key-Value pairs sorting function. Will sort by keys.
%% Args:    LhsPair - Left-hand-side Key-Value pair to compare.
%%          RhsPair - Right-hand-side Key-Value pair to compare.
%% Returns: true if LhsPair.Key < RhsPair.Key.
%%----------------------------------------------------------------------
store_sort({LhsKey, _}, {RhsKey, _}) ->
  LhsKey < RhsKey.

%%----------------------------------------------------------------------
%% Function: merge/2
%% Purpose: Adds the entries to the Storage. Overwrites old Values.
%% Args:    Entries - Storage to merge with.
%%          Storage - Current Storage.
%% Returns: UpdatedStorage.
%%----------------------------------------------------------------------
merge(Entries, Store) ->
  lists:foldl(
    fun({Key, Value}, AccStore) ->
      add(Key, Value, AccStore)
    end,
    Store,
    Entries
  ).

