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
    size :: file_location:model_record(),
    provider_id :: oneprovider:id(),
    sfm_handle :: storage_file_manager:handle(),
    file_guid :: fslogic_worker:file_guid(),
    open_flag :: fslogic_worker:open_flag(),
    session_id :: session:id()
}).

-type ctx() :: #lfm_context{}.
-type handle_id() :: binary() | undefined.

-export_type([ctx/0, handle_id/0]).

%% API
-export([new/7, set_size/2]).
-export([get_guid/1, get_session_id/1, get_share_id/1, get_provider_id/1,
    get_handle_id/1, get_sfm_handle/1, get_size/1, get_open_flag/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new lfm_context record.
%% @end
%%--------------------------------------------------------------------
-spec new(handle_id(), file_location:model_record(), od_provider:id(),
    storage_file_manager:handle(), session:id(), fslogic_worker:file_guid(),
    fslogic_worker:open_flag()) -> ctx().
new(HandleId, FileLocation, ProviderId, SfmHandle, SessionId, FileGuid, OpenFlag) ->
    #lfm_context{
        handle_id = HandleId,
        size = FileLocation,
        provider_id = ProviderId,
        sfm_handle = SfmHandle,
        session_id = SessionId,
        file_guid = FileGuid,
        open_flag = OpenFlag
    }.

%%--------------------------------------------------------------------
%% @doc
%% Sets file_location in handle
%% @end
%%--------------------------------------------------------------------
-spec set_size(ctx(), file_location:model_record()) -> ctx().
set_size(Handle, Size) ->
    Handle#lfm_context{size = Size}.

%%--------------------------------------------------------------------
%% @doc
%% Getx guid from context.
%% @end
%%--------------------------------------------------------------------
-spec get_guid(ctx()) -> fslogic_worker:file_guid().
get_guid(#lfm_context{file_guid = Guid}) ->
    Guid.

%%--------------------------------------------------------------------
%% @doc
%% Gets session id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(ctx()) -> session:id().
get_session_id(#lfm_context{session_id = SessionId}) ->
    SessionId.

%%--------------------------------------------------------------------
%% @doc
%% Gets share_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(ctx()) -> od_share:id() | undefined.
get_share_id(#lfm_context{file_guid = Guid}) ->
    fslogic_uuid:guid_to_share_id(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Gets provider_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id(ctx()) -> od_provider:id().
get_provider_id(#lfm_context{provider_id = ProviderId}) ->
    ProviderId.

%%--------------------------------------------------------------------
%% @doc
%% Gets handle_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_handle_id(ctx()) -> handle_id().
get_handle_id(#lfm_context{handle_id = HandleId}) ->
    HandleId.

%%--------------------------------------------------------------------
%% @doc
%% Gets sfm_handles from context.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handle(ctx()) -> storage_file_manager:handle().
get_sfm_handle(#lfm_context{sfm_handle = SfmHandle}) ->
    SfmHandle.

%%--------------------------------------------------------------------
%% @doc
%% Gets file_location from context.
%% @end
%%--------------------------------------------------------------------
-spec get_size(ctx()) -> file_location:model_record().
get_size(#lfm_context{size = Size}) ->
    Size.

%%--------------------------------------------------------------------
%% @doc
%% Gets open_flag from context.
%% @end
%%--------------------------------------------------------------------
-spec get_open_flag(ctx()) -> fslogic_worker:open_flag().
get_open_flag(#lfm_context{open_flag = OpenFlag}) ->
    OpenFlag.

%%%===================================================================
%%% Internal functions
%%%===================================================================