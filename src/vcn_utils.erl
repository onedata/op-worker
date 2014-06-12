%% ===================================================================
%% @author Konrad Zemek
%% @copyright (C): 2014, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: This module stores utility functions for use in other modules.
%% @end
%% ===================================================================
-module(vcn_utils).

%% API
-export([ensure_running/1, pmap/2, pforeach/2, time/0, record_type/1]).

%% ====================================================================
%% API functions
%% ====================================================================

%% ensure_running/1
%% ====================================================================
%% @doc Ensures that Application is started. See {@link application}
%% @end
-spec ensure_running(Application :: atom()) -> ok | {error, Reason :: term()}.
%% ====================================================================
ensure_running(Application) ->
    case application:start(Application) of
        {error, {already_started, Application}} -> ok;
        EverythingElse -> EverythingElse
    end.


%% pmap/2
%% ====================================================================
%% @doc A parallel version of lists:map/2. See {@link lists:map/2}
%% @end
-spec pmap(Fun :: fun((X :: A) -> B), L :: [A]) -> [B].
%% ====================================================================
pmap(Fun, L) ->
    Self = self(),
    Ref = erlang:make_ref(),
    PIDs = lists:map(fun(X) -> spawn(fun() -> pmap_f(Self, Ref, Fun, X) end) end, L),
    pmap_gather(PIDs, Ref, []).


%% pforeach/2
%% ====================================================================
%% @doc A parallel version of lists:foreach/2. See {@link lists:foreach/2}
%% @end
-spec pforeach(Fun :: fun((X :: A) -> any()), L :: [A]) -> ok.
%% ====================================================================
pforeach(Fun, L) ->
    Self = self(),
    Ref = erlang:make_ref(),
    lists:foreach(fun(X) -> spawn(fun() -> pforeach_f(Self, Ref, Fun, X) end) end, L),
    pforeach_gather(length(L), Ref).


%% ====================================================================
%% Internal functions
%% ====================================================================

%% pmap_f/4
%% ====================================================================
%% @doc Runs a function on X and returns its result to parent.
%% @end
-spec pmap_f(Parent :: pid(), Ref :: reference(), Fun :: fun((E :: A) -> any()), X :: A) -> {pid(), reference(), term()}.
%% ====================================================================
pmap_f(Parent, Ref, Fun, X) -> Parent ! {self(), Ref, (catch Fun(X))}.


%% pmap_gather/3
%% ====================================================================
%% @doc Gathers the results of pmap.
%% @end
-spec pmap_gather(PIDs :: [pid()], Ref :: reference(), Acc :: list()) -> list().
%% ====================================================================
pmap_gather([], _Ref, Acc) -> lists:reverse(Acc);
pmap_gather([PID | T], Ref, Acc) ->
    receive
        {PID, Ref, Result} -> pmap_gather(T, Ref, [Result | Acc])
    end.


%% pforeach_f/4
%% ====================================================================
%% @doc Runs a function on X and signals parent that it's done.
%% @end
-spec pforeach_f(Parent :: pid(), Ref :: reference(), Fun :: fun((E :: A) -> any()), X :: A) -> reference().
%% ====================================================================
pforeach_f(Parent, Ref, Fun, X) -> catch Fun(X), Parent ! Ref.


%% pforeach_gather/2
%% ====================================================================
%% @doc Joins pforeach processes.
%% @end
-spec pforeach_gather(PIDs :: [pid()], Ref :: reference()) -> ok.
%% ====================================================================
pforeach_gather(0, _Ref) -> ok;
pforeach_gather(N, Ref) ->
    receive
        Ref -> pforeach_gather(N - 1, Ref)
    end.

%% time/0
%% ====================================================================
%% @doc Returns time in seconds.
%% @end
-spec time() -> Result :: integer().
time() ->
    {M, S, _} = now(),
    M * 1000000 + S.


%% record_type/1
%% ====================================================================
%% @doc Gets record type for given record. Since the is now way of knowing whether
%%      given tuple is record, this method behaviour is unspecified for non-record tuples.
%% @end
-spec record_type(Record :: tuple()) ->
    atom() | no_return().
%% ====================================================================
record_type(Record) when is_tuple(Record) ->
    element(1, Record).