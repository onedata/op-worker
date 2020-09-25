%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Model holding information about process (gui/cdmi) opened lfm handles.
%%% @end
%%%-------------------------------------------------------------------
-module(process_handles).
-author("Bartosz Walkowicz").

-include("modules/datastore/datastore_models.hrl").
-include("proto/oneclient/common_messages.hrl").

% API
-export([
    add/1, remove/1,
    get_all_process_handles/1,
    release_all_process_handles/1
]).
-export([release_all_dead_processes_handles/0]).

%% datastore_model callbacks
-export([get_ctx/0]).

-type id() :: datastore:key().
-type record() :: #process_handles{}.
-type doc() :: datastore_doc:doc(record()).

-export_type([id/0, record/0, doc/0]).


-define(CTX, #{
    model => ?MODULE,
    disc_driver => undefined,
    fold_enabled => true
}).


%%%===================================================================
%%% API
%%%===================================================================


-spec add(lfm:handle()) -> ok | {error, term()}.
add(FileHandle) ->
    Process = self(),
    HandleId = lfm_context:get_handle_id(FileHandle),

    Diff = fun(#process_handles{handles = ProcessHandles} = Record) ->
        {ok, Record#process_handles{
            process = Process,
            handles = ProcessHandles#{HandleId => FileHandle}}
        }
    end,
    {ok, Default} = Diff(#process_handles{}),

    case datastore_model:update(?CTX, key(Process), Diff, Default) of
        {ok, #document{value = #process_handles{handles = Handles}}} when map_size(Handles) == 1 ->
            lfm_handles_monitor:monitor_process(Process);
        {ok, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.


-spec remove(lfm:handle()) -> ok | {error, term()}.
remove(FileHandle) ->
    Process = self(),
    Key = key(Process),

    Diff = fun(#process_handles{handles = ProcessHandles} = Record) ->
        HandleId = lfm_context:get_handle_id(FileHandle),
        {ok, Record#process_handles{handles = maps:remove(HandleId, ProcessHandles)}}
    end,
    case datastore_model:update(?CTX, Key, Diff) of
        {ok, #document{value = #process_handles{handles = Handles}}} when map_size(Handles) == 0 ->
            lfm_handles_monitor:demonitor_process(Process),
            delete_doc(Key);
        {ok, _}  ->
            ok;
        ?ERROR_NOT_FOUND ->
            ok;
        {error, _} = Error ->
            Error
    end.


-spec get_all_process_handles(pid()) -> {ok, [lfm:handle()]} | {error, term()}.
get_all_process_handles(Process) ->
    case get_doc(key(Process)) of
        {ok, #document{value = #process_handles{handles = ProcessHandles}}} ->
            {ok, maps:values(ProcessHandles)};
        {error, _} = Error ->
            Error
    end.


-spec release_all_process_handles(pid()) -> ok | {error, term()}.
release_all_process_handles(Process) ->
    case get_all_process_handles(Process) of
        {ok, ProcessHandles} ->
            release_handles(ProcessHandles),
            delete_doc(key(Process));
        ?ERROR_NOT_FOUND ->
            ok;
        {error, _} = Error ->
            Error
    end.


-spec release_all_dead_processes_handles() -> ok | {error, term()}.
release_all_dead_processes_handles() ->
    case list_all_docs() of
        {ok, AllDocs} ->
            lists:foreach(fun(#document{
                key = Key,
                value = #process_handles{process = Process, handles = Handles}
            }) ->
                case is_alive(Process) of
                    true ->
                        ok;
                    false ->
                        release_handles(maps:values(Handles)),
                        delete_doc(Key)
                end
            end, AllDocs);
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec key(pid()) -> binary().
key(Process) ->
    EncodedPid = term_to_binary(Process),
    datastore_key:adjacent_from_digest([EncodedPid], EncodedPid).


%% @private
-spec list_all_docs() -> {ok, [doc()]} | {error, term()}.
list_all_docs() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).


%% @private
-spec get_doc(id()) -> {ok, doc()} | {error, term()}.
get_doc(Key) ->
    datastore_model:get(?CTX, Key).


%% @private
-spec delete_doc(id()) -> ok | {error, term()}.
delete_doc(Key) ->
    datastore_model:delete(?CTX, Key).


%% @private
-spec is_alive(pid()) -> boolean().
is_alive(Pid) ->
    try rpc:pinfo(Pid, [status]) of
        [{status, _}] -> true;
        _ -> false
    catch _:_ ->
        false
    end.


%% @private
-spec release_handles([lfm:handle()]) -> ok.
release_handles(ProcessHandles) ->
    lists:foreach(fun(FileHandle) -> lfm:release(FileHandle) end, ProcessHandles).


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
