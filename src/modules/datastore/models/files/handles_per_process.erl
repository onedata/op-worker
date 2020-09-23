%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model holding information about opened lfm handles per process (gui/REST).
%%% @end
%%%-------------------------------------------------------------------
-module(handles_per_process).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").

% API
-export([
    register_handle/2,
    deregister_handle/2,
    release_all_process_handles/1
]).
-export([release_all_dead_processes_handles/0]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1]).

-type id() :: datastore:key().
-type record() :: #handles_per_process{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    fold_enabled => true,
    memory_copies => all
}).


-define(KEY, <<"OPENED_LFM_HADNLES_PER_PROCESS">>).


%%%===================================================================
%%% API
%%%===================================================================


-spec register_handle(pid(), lfm:handle()) -> ok | {error, term()}.
register_handle(Pid, FileHandle) ->
    HandleId = lfm_context:get_handle_id(FileHandle),

    Diff = fun(#handles_per_process{handles = HandlesPerProcess} = Record) ->
        ProcessHandles = maps:get(Pid, HandlesPerProcess, #{}),
        {ok, Record#handles_per_process{handles = HandlesPerProcess#{
            Pid => ProcessHandles#{HandleId => FileHandle}
        }}}
    end,
    Default = #document{
        key = ?KEY,
        value = #handles_per_process{handles = #{Pid => #{HandleId => FileHandle}}}
    },
    case datastore_model:update(?CTX, ?KEY, Diff, Default) of
        {ok, _} -> ok;
        {error, _} = Error -> Error
    end.


-spec deregister_handle(pid(), lfm:handle()) ->
    {ok, LeftoverProcessHandles :: [lfm:handle()]} | {error, term()}.
deregister_handle(Pid, FileHandle) ->
    HandleId = lfm_context:get_handle_id(FileHandle),

    Diff = fun(#handles_per_process{handles = HandlesPerProcess} = Record) ->
        ProcessHandles = maps:get(Pid, HandlesPerProcess, #{}),
        case maps:remove(HandleId, ProcessHandles) of
            LeftoverProcessHandles when map_size(LeftoverProcessHandles) == 0 ->
                {ok, Record#handles_per_process{
                    handles = maps:remove(Pid, HandlesPerProcess)
                }};
            LeftoverProcessHandles ->
                {ok, Record#handles_per_process{handles = HandlesPerProcess#{
                    Pid => LeftoverProcessHandles
                }}}
        end
    end,
    case datastore_model:update(?CTX, ?KEY, Diff) of
        {ok, #document{value = #handles_per_process{handles = HandlesPerProcess}}} ->
            {ok, maps:values(maps:get(Pid, HandlesPerProcess, #{}))};
        ?ERROR_NOT_FOUND ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.


-spec release_all_process_handles(pid()) -> ok | {error, term()}.
release_all_process_handles(Pid) ->
    case datastore_model:get(?CTX, ?KEY) of
        {ok, #document{value = #handles_per_process{handles = HandlesPerProcess}}} ->
            release_handles(maps:values(maps:get(Pid, HandlesPerProcess, #{}))),

            Diff = fun(#handles_per_process{handles = HandlesPerProcess} = Record) ->
                {ok, Record#handles_per_process{handles = maps:remove(Pid, HandlesPerProcess)}}
            end,
            case datastore_model:update(?CTX, ?KEY, Diff) of
                {ok, _} -> ok;
                ?ERROR_NOT_FOUND -> ok;
                {error, Reason} -> {error, Reason}
            end;
        ?ERROR_NOT_FOUND ->
            ok;
        {error, _} = Error ->
            Error
    end.


-spec release_all_dead_processes_handles() -> ok | {error, term()}.
release_all_dead_processes_handles() ->
    case datastore_model:get(?CTX, ?KEY) of
        {ok, #document{value = #handles_per_process{handles = HandlesPerProcess}}} ->
            DeadProcesses = lists:foldl(fun({Pid, ProcessHandles}, DeadProcessesAcc) ->
                case is_alive(Pid) of
                    true ->
                        DeadProcessesAcc;
                    false ->
                        release_handles(maps:values(ProcessHandles)),
                        [Pid | DeadProcessesAcc]
                end
            end, [], maps:to_list(HandlesPerProcess)),

            case DeadProcesses of
                [] ->
                    ok;
                _ ->
                    Diff = fun(#handles_per_process{handles = HandlesPerProcess} = Record) ->
                        {ok, Record#handles_per_process{
                            handles = maps:without(DeadProcesses, HandlesPerProcess)
                        }}
                    end,
                    case datastore_model:update(?CTX, ?KEY, Diff) of
                        {ok, _} -> ok;
                        ?ERROR_NOT_FOUND -> ok;
                        {error, Reason} -> {error, Reason}
                    end
            end;
        ?ERROR_NOT_FOUND ->
            ok;
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_version() -> datastore_model:record_version().
get_record_version() ->
    1.


%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {handles, #{string => #{string => term}}}
    ]}.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec release_handles([lfm:handle()]) -> ok.
release_handles(ProcessHandles) ->
    lists:foreach(fun(FileHandle) -> lfm:release(FileHandle) end, ProcessHandles).


%% @private
-spec is_alive(pid()) -> boolean().
is_alive(Pid) ->
    try rpc:pinfo(Pid, [status]) of
        [{status, _}] -> true;
        _ -> false
    catch _:_ ->
        false
    end.
