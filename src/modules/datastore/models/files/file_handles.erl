%%%-------------------------------------------------------------------
%%% @author Michal Wrona
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that stores open files.
%%% @end
%%%-------------------------------------------------------------------
-module(file_handles).
-author("Michal Wrona").
-author("Krzysztof Trzepla").

-include("modules/datastore/datastore_models.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([delete/1, exists/1, list/0]).
-export([register_open/3, register_release/3, mark_to_remove/1,
    invalidate_session_entry/2]).

%% datastore_model callbacks
-export([get_ctx/0]).
-export([get_record_version/0, get_record_struct/1, upgrade_record/2]).

-type key() :: datastore:key().
-type record() :: #file_handles{}.
-type doc() :: datastore_doc:doc(record()).

-define(CTX, #{
    model => ?MODULE,
    fold_enabled => true
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Deletes file handle.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether file handle exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(key()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [doc()]} | {error, term()}.
list() ->
    datastore_model:fold(?CTX, fun(Doc, Acc) -> {ok, [Doc | Acc]} end, []).

%%--------------------------------------------------------------------
%% @doc
%% Registers number given in Count of new file descriptors for given
%% FileCtx and SessionId.
%% @end
%%--------------------------------------------------------------------
-spec register_open(file_ctx:ctx(), session:id(), pos_integer()) ->
    ok | {error, term()}.
register_open(FileCtx, SessId, Count) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Diff = fun
        (#file_handles{is_removed = true}) ->
            {error, removed};
        (Handle = #file_handles{descriptors = Fds}) ->
            FdCount = maps:get(SessId, Fds, 0),
            {ok, Handle#file_handles{
                descriptors = maps:put(SessId, FdCount + Count, Fds)
            }}
    end,
    Diff2 = fun
        (Handle = #file_handles{descriptors = Fds}) ->
            case maps:get(SessId, Fds, 0) - Count of
                0 ->
                    {ok, Handle#file_handles{
                        descriptors = maps:remove(SessId, Fds)
                    }};
                FdCount ->
                    {ok, Handle#file_handles{
                        descriptors = maps:put(SessId, FdCount, Fds)
                    }}
            end
    end,
    Default = #document{key = FileUuid, value = #file_handles{
        descriptors = #{SessId => Count}
    }},
    case datastore_model:update(?CTX, FileUuid, Diff, Default) of
        {ok, _} ->
            case session:add_open_file(SessId, FileGuid) of
                ok ->
                    ok;
                {error, Reason} ->
                    datastore_model:update(?CTX, FileUuid, Diff2),
                    {error, Reason}
            end;
        {error, removed} ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Removes number given in Count of file descriptors for given
%% FileUuid and SessionId. Removes file if no file descriptor
%% is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec register_release(file_ctx:ctx(), session:id(), pos_integer() | infinity) ->
    ok | {error, term()}.
register_release(FileCtx, SessId, Count) ->
    FileUuid = file_ctx:get_uuid_const(FileCtx),
    FileGuid = file_ctx:get_guid_const(FileCtx),
    Diff = fun(Handle = #file_handles{is_removed = Removed, descriptors = Fds}) ->
        FdCount = maps:get(SessId, Fds, 0),
        case Count =:= infinity orelse FdCount =< Count of
            true ->
                Fds2 = maps:remove(SessId, Fds),
                case {Removed, maps:size(Fds2)} of
                    {true, 0} -> {error, removed};
                    _ -> {ok, Handle#file_handles{descriptors = Fds2}}
                end;
            false ->
                {ok, Handle#file_handles{
                    descriptors = maps:put(SessId, FdCount - Count, Fds)
                }}
        end
    end,
    case datastore_model:update(?CTX, FileUuid, Diff) of
        {ok, #document{value = #file_handles{descriptors = Fds}}} ->
            ?info("mmmmm1 ~p", [{FileCtx, Fds}]),
            case maps:is_key(SessId, Fds) of
                true -> ok;
                false -> session:remove_open_file(SessId, FileGuid)
            end,
            Pred = fun(#file_handles{descriptors = Fds2}) ->
                maps:size(Fds2) == 0
            end,
            case datastore_model:delete(?CTX, FileUuid, Pred) of
                ok -> ok;
                {error, {not_satisfied, _}} -> ok;
                {error, Reason} -> {error, Reason}
            end;
        {error, removed} ->
            ?info("mmmmm2 ~p", [{FileCtx}]),
            session:remove_open_file(SessId, FileGuid),
            fslogic_deletion_worker:request_open_file_deletion(FileCtx),
            datastore_model:delete(?CTX, FileUuid);
        {error, not_found} ->
            ?info("mmmmm3 ~p", [{FileCtx}]),
            ok;
        {error, Reason} ->
            ?info("mmmmm4 ~p", [{FileCtx, Reason}]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Marks files as removed.
%% @end
%%--------------------------------------------------------------------
-spec mark_to_remove(file_ctx:ctx()) -> ok | {error, term()}.
mark_to_remove(FileCtx) ->
    Diff = fun(Handle = #file_handles{}) ->
        {ok, Handle#file_handles{is_removed = true}}
    end,
    case datastore_model:update(?CTX, file_ctx:get_uuid_const(FileCtx), Diff) of
        {ok, _} -> ok;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Clears descriptors count associated with SessionId for given FileUuid.
%% Removes file if no file descriptor is active and file is marked as removed.
%% @end
%%--------------------------------------------------------------------
-spec invalidate_session_entry(file_ctx:ctx(), session:id()) ->
    ok | {error, term()}.
invalidate_session_entry(FileCtx, SessId) ->
    case register_release(FileCtx, SessId, infinity) of
        ok -> ok;
        {error, not_found} -> ok;
        {error, Reason} -> {error, Reason}
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
    2.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {is_removed, boolean},
        {active_descriptors, #{string => integer}}
    ]};
get_record_struct(2) ->
    {record, Struct} = get_record_struct(1),
    {record, lists:keyreplace(
        active_descriptors, 1, Struct, {descriptors, #{string => integer}}
    )}.

%%--------------------------------------------------------------------
%% @doc
%% Upgrades model's record from provided version to the next one.
%% @end
%%--------------------------------------------------------------------
-spec upgrade_record(datastore_model:record_version(), datastore_model:record()) ->
    {datastore_model:record_version(), datastore_model:record()}.
upgrade_record(1, {?MODULE, IsRemoved, Descriptors}) ->
    {2, #file_handles{is_removed = IsRemoved, descriptors = Descriptors}}.