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
    file_location :: file_location:model_record(),
    provider_id :: oneprovider:id(),
    sfm_handles = #{} :: sfm_handles_map(),
    file_guid :: fslogic_worker:file_guid(),
    open_flag :: fslogic_worker:open_flag(),
    session_id :: session:id()
}).

-type sfm_handles_map() :: #{term() => {term(), storage_file_manager:handle()}}.
-type handle() :: #lfm_context{}.
-type handle_id() :: binary() | undefined.

-export_type([handle/0, handle_id/0]).

%% API
-export([new/7, set_file_location/2, set_sfm_handles/2]).
-export([get_guid/1, get_session_id/1, get_share_id/1, get_provider_id/1,
    get_handle_id/1, get_sfm_handles/1, get_file_location/1, get_open_flag/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new lfm_context record.
%% @end
%%--------------------------------------------------------------------
-spec new(handle_id(), file_location:model_record(), od_provider:id(),
    sfm_handles_map(), session:id(), fslogic_worker:file_guid(),
    fslogic_worker:open_flag()) -> handle().
new(HandleId, FileLocation, ProviderId, SfmHandles, SessionId, FileGuid, OpenFlag) ->
    #lfm_context{
        handle_id = HandleId,
        file_location = FileLocation,
        provider_id = ProviderId,
        sfm_handles = SfmHandles,
        session_id = SessionId,
        file_guid = FileGuid,
        open_flag = OpenFlag
    }.

%%--------------------------------------------------------------------
%% @doc
%% Sets file_location in handle
%% @end
%%--------------------------------------------------------------------
-spec set_file_location(handle(), file_location:model_record()) -> handle().
set_file_location(Handle, FileLocation) ->
    Handle#lfm_context{file_location = FileLocation}.

%%--------------------------------------------------------------------
%% @doc
%% Sets sfm_handles in handle
%% @end
%%--------------------------------------------------------------------
-spec set_sfm_handles(handle(), sfm_handles_map()) -> handle().
set_sfm_handles(Handle, FileLocation) ->
    Handle#lfm_context{sfm_handles = FileLocation}.

%%--------------------------------------------------------------------
%% @doc
%% Getx guid from context.
%% @end
%%--------------------------------------------------------------------
-spec get_guid(handle()) -> fslogic_worker:file_guid().
get_guid(#lfm_context{file_guid = Guid}) ->
    Guid.

%%--------------------------------------------------------------------
%% @doc
%% Gets session id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_session_id(handle()) -> session:id().
get_session_id(#lfm_context{session_id = SessionId}) ->
    SessionId.

%%--------------------------------------------------------------------
%% @doc
%% Gets share_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(handle()) -> od_share:id() | undefined.
get_share_id(#lfm_context{file_guid = Guid}) ->
    fslogic_uuid:guid_to_share_id(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Gets provider_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_provider_id(handle()) -> od_provider:id().
get_provider_id(#lfm_context{provider_id = ProviderId}) ->
    ProviderId.

%%--------------------------------------------------------------------
%% @doc
%% Gets handle_id from context.
%% @end
%%--------------------------------------------------------------------
-spec get_handle_id(handle()) -> handle_id().
get_handle_id(#lfm_context{handle_id = HandleId}) ->
    HandleId.

%%--------------------------------------------------------------------
%% @doc
%% Gets sfm_handles from context.
%% @end
%%--------------------------------------------------------------------
-spec get_sfm_handles(handle()) -> sfm_handles_map().
get_sfm_handles(#lfm_context{sfm_handles = SfmHandles}) ->
    SfmHandles.

%%--------------------------------------------------------------------
%% @doc
%% Gets file_location from context.
%% @end
%%--------------------------------------------------------------------
-spec get_file_location(handle()) -> file_location:model_record().
get_file_location(#lfm_context{file_location = FileLocation}) ->
    FileLocation.

%%--------------------------------------------------------------------
%% @doc
%% Gets open_flag from context.
%% @end
%%--------------------------------------------------------------------
-spec get_open_flag(handle()) -> fslogic_worker:open_flag().
get_open_flag(#lfm_context{open_flag = OpenFlag}) ->
    OpenFlag.

%%%===================================================================
%%% Internal functions
%%%===================================================================