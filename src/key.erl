%%%-------------------------------------------------------------------
%%% @author fabi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. Oct 2020 15:51
%%%-------------------------------------------------------------------
-module(key).
-author("fabi").

%% API
-export([generate/0, between/3]).

generate() ->
  rand:uniform(1000000000).

between(_, From, From) ->
  % Full circle. Return true.
  true;
between(Key, From, To) when From < To ->
  % Normal interval.
  (From < Key) and (Key =< To);
between(Key, From, To) when To < From ->
  % Since the interval is laid on a circle, we might run into scenarios in which
  % we want to check the interval (7, 3].
  (From < Key) or (Key =< To).


