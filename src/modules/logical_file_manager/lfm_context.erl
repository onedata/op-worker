%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Wrapper for lfm_context record, allowing to get and set its data.
%%% @end
%%%--------------------------------------------------------------------
-module(lfm_context).
-author("Tomasz Lichon").

%% Internal opaque file-handle used by logical_file_manager
-record(lfm_context, {
    handle_id :: lfm_context:handle_id(),
    provider_id :: oneprovider:id(),
    file_guid :: fslogic_worker:file_guid(),
    open_flag :: fslogic_worker:open_flag(),
    session_id :: session:id(),
    file_id :: helpers:file_id(),
    storage_id :: storage:id()
}).

-type ctx() :: #lfm_context{}.
-type handle_id() :: binary() | undefined.

-export_type([ctx/0, handle_id/0]).

%% API
-export([new/7]).
-export([get_guid/1, get_session_id/1, get_share_id/1, get_provider_id/1,
    get_handle_id/1, get_file_id/1, get_storage_id/1, get_open_flag/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new lfm_context record.
%% @end
%%--------------------------------------------------------------------
-spec new(handle_id(), od_provider:id(), session:id(),
    fslogic_worker:file_guid(), fslogic_worker:open_flag(),
    helpers:file_id(), storage:id()) -> ctx().
new(HandleId, ProviderId, SessionId, FileGuid, OpenFlag, FileId, StorageId) ->
    #lfm_context{
        handle_id = HandleId,
        provider_id = ProviderId,
        session_id = SessionId,
        file_guid = FileGuid,
        open_flag = OpenFlag,
        file_id = FileId,
        storage_id = StorageId
    }.

%%--------------------------------------------------------------------
%% @doc
%% Returns guid from context.
%% @end
%%--------------------------------------------------------------------
-spec get_guid(ctx()) -> fslogic_worker:file_guid().
get_guid(#lfm_context{file_guid = Guid}) ->
    Guid.

%%--------------------------------------------------------------------
%% @doc
%% Returns session id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(ctx()) -> session:id().
get_session_id(#lfm_context{session_id = SessionId}) ->
    SessionId.

%%--------------------------------------------------------------------
%% @doc
%% Returns share_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(ctx()) -> od_share:id() | undefined.
get_share_id(#lfm_context{file_guid = Guid}) ->
    fslogic_uuid:guid_to_share_id(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Returns provider_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id(ctx()) -> od_provider:id().
get_provider_id(#lfm_context{provider_id = ProviderId}) ->
    ProviderId.

%%--------------------------------------------------------------------
%% @doc
%% Returns handle_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_handle_id(ctx()) -> handle_id().
get_handle_id(#lfm_context{handle_id = HandleId}) ->
    HandleId.

%%--------------------------------------------------------------------
%% @doc
%% Returns file_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_file_id(ctx()) -> helpers:file_id().
get_file_id(#lfm_context{file_id = FileId}) ->
    FileId.

%%--------------------------------------------------------------------
%% @doc
%% Returns storage_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_storage_id(ctx()) -> storage:id().
get_storage_id(#lfm_context{storage_id = StorageId}) ->
    StorageId.

%%--------------------------------------------------------------------
%% @doc
%% Returns open_flag from context.
%% @end
%%--------------------------------------------------------------------
-spec get_open_flag(ctx()) -> fslogic_worker:open_flag().
get_open_flag(#lfm_context{open_flag = OpenFlag}) ->
    OpenFlag.

%%%===================================================================
%%% Internal functions
%%%===================================================================