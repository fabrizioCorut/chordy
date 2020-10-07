%%%-------------------------------------------------------------------
%%% @author fabi
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% Representation of a potential hash. Ideally unique everytime generate is called, within a bound.
%%% @end
%%% Created : 03. Oct 2020 15:51
%%%-------------------------------------------------------------------
-module(key).
-author("fabi").

% Defines the maximum number up to which, starting from 1, we want to generate a random number from.
-define(MaximumHash, 1000000000).

%% API
-export([generate/0, between/3]).

%%----------------------------------------------------------------------
%% Function: generate/0
%% Purpose: Generates a unique number that will serve as a key into a hash-table.
%%----------------------------------------------------------------------
generate() ->
  rand:uniform(?MaximumHash).

%%----------------------------------------------------------------------
%% Function: between/3
%% Purpose: Determines whether a Key is found in the ring interval formed by From and To. It's excluse at the lower and inclusive at the higher end.
%%          Ring interval is when the next element after the last, from a linear interval, is the first one, thus forming a ring.
%%          e.g. ring containing numbers from 0 to 8. A valid interval can be : (4, 1].
%% Args:    Key - To determine whether or not it's found between To and From.
%%          To - Start of the interval.
%%          From - End of the interval.
%% Returns: true/ false.
%%----------------------------------------------------------------------
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


