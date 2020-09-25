%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Helper module for storage_import_worker.
%%% It implements a simple registry for tracking spaces with
%%% enabled auto-import.
%%%
%%% WARNING!!!
%%% This module should only be used by just one process to avoid
%%% race conditions.
%%% @end
%%%-------------------------------------------------------------------
-module(auto_imported_spaces_registry).
-author("Jakub Kudzia").

-include("modules/storage/import/storage_import.hrl").
-include("modules/storage/import/utils/auto_imported_spaces_registry.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/errors.hrl").

%% API
-export([ensure_initialized/0, revise/0]).
-export([register/1, deregister/1]).
-export([mark_inactive/1, mark_scanning/1]).
-export([fold/2]).

-define(REGISTRY, ?MODULE).

-type registry_status() :: ?INITIALIZED | ?NOT_INITIALIZED.
-type key() :: od_space:id().
-type value() :: ?SCANNING | ?INACTIVE.
-type fold_fun() :: fun((key(), value(), AccIn :: term()) -> term()).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec ensure_initialized() -> registry_status().
ensure_initialized() ->
    case is_created() of
        false ->
            init();
        true ->
            ?INITIALIZED
    end.

%%--------------------------------------------------------------------
%% @doc
%% This function is used to revise spaces stored in ets.
%% It ensures whether all supported spaces are in the ets and removes
%% stalled (no longer supported) spaces from it.
%%
%% In normal operation this function should do nothing as adding/removing
%% spaces to/from the registry should be performed by storage_import_worker.
%% This function is meant to revise the registry in case of unexpected
%% problems.
%% @end
%%--------------------------------------------------------------------
-spec revise() -> ok.
revise() ->
    case is_created() of
        false ->
            init();
        true ->
            case provider_logic:get_spaces() of
                {ok, Spaces} ->
                    CurrentlyRegisteredSpaces = list(),
                    revise(lists:sort(Spaces), lists:sort(CurrentlyRegisteredSpaces));
                ?ERROR_NO_CONNECTION_TO_ONEZONE ->
                    ?debug("storage_import_worker was unable to revise its registry due to no connection to oz.");
                ?ERROR_UNREGISTERED_ONEPROVIDER ->
                    ?debug("storage_import_worker was unable to revise its registry due to unregistered provider.");
                {error, _} = Error ->
                    ?error("storage_import_worker was unable to revise its registry due to unexpected ~p", [Error])
            end
    end.


-spec register(key()) -> ok.
register(SpaceId) ->
    case is_created() of
        true ->
            check_if_auto_imported_and_register(SpaceId);
        false ->
            init(),
            ok
    end.


-spec deregister(key()) -> ok.
deregister(SpaceId) ->
    case is_created() of
        true ->
            deregister_internal(SpaceId);
        false ->
            ok
    end.


-spec mark_inactive(key()) -> ok.
mark_inactive(SpaceId) ->
    mark_status(SpaceId, ?INACTIVE).


-spec mark_scanning(key()) -> ok.
mark_scanning(SpaceId) ->
    mark_status(SpaceId, ?SCANNING).


-spec fold(fold_fun(), term()) -> {ok, term()}.
fold(Fun, AccIn) ->
    fold(ets:first(?REGISTRY), Fun, AccIn).

%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec init() -> registry_status().
init() ->
    try
        case provider_logic:get_spaces() of
            {ok, Spaces} ->
                create_empty(),
                register_auto_imported_spaces(Spaces),
                ?INITIALIZED;
            ?ERROR_NO_CONNECTION_TO_ONEZONE ->
                ?debug("storage_import_worker was unable to collect auto imported spaces due to no connection to oz"),
                ?NOT_INITIALIZED;
            ?ERROR_UNREGISTERED_ONEPROVIDER ->
                ?debug("storage_import_worker was unable to collect auto imported spaces due to unregistered provider"),
                ?NOT_INITIALIZED;
            {error, _} = Error ->
                ?error("storage_import_worker was unable to collect auto imported spaces due to unexpected ~p", [Error]),
                ?NOT_INITIALIZED
        end
    catch
        Error2:Reason ->
            ?error_stacktrace("storage_import_worker was unable to collect auto imported spaces due to unexpected ~p:~p", [Error2, Reason]),
            catch ets:delete(?REGISTRY),
            ?NOT_INITIALIZED
    end.


-spec create_empty() -> ok.
create_empty() ->
    ?REGISTRY = ets:new(?REGISTRY, [named_table, public]),
    ok.


-spec register_internal(key()) -> ok.
register_internal(SpaceId) ->
    true = ets:insert(?REGISTRY, {SpaceId, ?INACTIVE}),
    ok.


-spec deregister_internal(key()) -> ok.
deregister_internal(SpaceId) ->
    true = ets:delete(?REGISTRY, SpaceId),
    ok.


-spec is_created() -> boolean().
is_created() ->
    ets:info(?REGISTRY) =/= undefined.


-spec mark_status(key(), value()) -> ok.
mark_status(SpaceId, Status) ->
    case is_created() of
        true ->
            ets:update_element(?REGISTRY, SpaceId, {2, Status}),
            ok;
        false ->
            case init() of
                ?INITIALIZED -> mark_status(SpaceId, Status);
                ?NOT_INITIALIZED -> ok
            end
    end.


-spec register_auto_imported_spaces([od_space:id()]) -> ok.
register_auto_imported_spaces(Spaces) ->
    lists:foreach(fun(SpaceId) ->
        check_if_auto_imported_and_register(SpaceId)
    end, Spaces).


-spec check_if_auto_imported_and_register(od_space:id()) -> ok.
check_if_auto_imported_and_register(SpaceId) ->
    try
        case storage_import:get_mode(SpaceId) of
            {ok, ?AUTO_IMPORT} ->
                register_internal(SpaceId);
            {ok, _} ->
                ok;
            {error, not_found} ->
                ok;
            {error, _} = Error ->
                ?error("Could not check if space ~s is auto imported due to ~p", [SpaceId, Error])
        end
    catch
        E:R ->
            ?error_stacktrace("Could not check if space ~s is auto imported due to unexpected ~p:~p", [SpaceId, E, R])
    end.


-spec revise(NewSpaces :: [od_space:id()], PreviousSpaces :: [od_space:id()]) -> ok.
revise([], []) ->
    ok;
revise([SpaceId | NewSpaces], [SpaceId | PreviousSpaces]) ->
    revise(NewSpaces, PreviousSpaces);
revise([SpaceId | NewSpaces], PreviousSpaces) ->
    register(SpaceId),
    revise(NewSpaces, PreviousSpaces);
revise(NewSpaces, [SpaceId | PreviousSpaces]) ->
    deregister(SpaceId),
    revise(NewSpaces, PreviousSpaces).


-spec list() -> [od_space:id()].
list() ->
    {ok, Spaces} = fold(fun(SpaceId, _, AccIn) ->
        [SpaceId | AccIn]
    end, []),
    Spaces.


-spec fold(key() | '$end_of_table', fold_fun(), AccIn :: term()) -> {ok, term()}.
fold('$end_of_table', _Fun, AccIn) ->
    {ok, AccIn};
fold(SpaceId, Fun, AccIn) ->
    [{SpaceId, Status}] = ets:lookup(?REGISTRY, SpaceId),
    Acc = Fun(SpaceId, Status, AccIn),
    fold(ets:next(?REGISTRY, SpaceId), Fun, Acc).