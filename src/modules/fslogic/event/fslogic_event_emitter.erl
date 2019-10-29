%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Module responsible for pushing new file's information to sessions.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_event_emitter).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/oneclient/server_messages.hrl").
-include("timeouts.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_file_attr_changed/2, emit_file_attr_changed/3, emit_sizeless_file_attrs_changed/1,
    emit_file_location_changed/2, emit_file_location_changed/3,
    emit_file_location_changed/4, emit_file_locations_changed/2,
    emit_file_perm_changed/1, emit_file_removed/2,
    emit_file_renamed_to_client/3, emit_quota_exceeded/0,
    emit_helper_params_changed/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_changed(file_ctx:ctx(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed(FileCtx, ExcludedSessions) ->
    case file_ctx:get_and_cache_file_doc_including_deleted(FileCtx) of
        {error, not_found} ->
            ok;
        {#document{}, FileCtx2} ->
            #fuse_response{fuse_response = #file_attr{} = FileAttr} =
                attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx2, true),
            emit_file_attr_changed(FileCtx2, FileAttr, ExcludedSessions);
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_changed(file_ctx:ctx(), #file_attr{}, [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_changed(FileCtx, FileAttr, ExcludedSessions) ->
    {ParentGuid, _} = file_ctx:get_parent_guid(FileCtx, undefined),
    event:get_subscribers_and_emit(#file_attr_changed_event{file_attr = FileAttr},
        ParentGuid, ExcludedSessions).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes excluding size to all subscribers.
%% @end
%%--------------------------------------------------------------------
-spec emit_sizeless_file_attrs_changed(file_ctx:ctx()) ->
    ok | {error, Reason :: term()}.
emit_sizeless_file_attrs_changed(FileCtx) ->
    case file_ctx:get_and_cache_file_doc_including_deleted(FileCtx) of
        {error, not_found} ->
            ok;
        {#document{}, FileCtx2} ->
            #fuse_response{fuse_response = #file_attr{} = FileAttr} =
                attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID),
                    FileCtx2, true, false),
            {ParentGuid, _} = file_ctx:get_parent_guid(FileCtx2, undefined),
            event:get_subscribers_and_emit(#file_attr_changed_event{
                file_attr = FileAttr
            }, ParentGuid, []);
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% @doc
%% @equiv emit_file_location_changed(FileEntry, ExcludedSessions, undefined)
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_ctx:ctx(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(FileCtx, ExcludedSessions) ->
    emit_file_location_changed(FileCtx, ExcludedSessions, undefined).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list. The given range tells what range is requested,
%% so we may fill the gaps within, id defaults to whole file.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_ctx:ctx(), [session:id()],
    fslogic_blocks:blocks() | fslogic_blocks:block() | undefined) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(FileCtx, ExcludedSessions, Range) ->
    {Location, _FileCtx2} = file_ctx:get_file_location_with_filled_gaps(FileCtx, Range),
    {Offset, Size} = fslogic_location_cache:get_blocks_range(Location, Range),
    emit_file_location_changed(Location, ExcludedSessions, Offset, Size).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list. The given range tells what range specifies which
%% range of blocks should be included in event.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_changed(file_location:record(), [session:id()],
    non_neg_integer() | undefined, non_neg_integer() | undefined) ->
    ok | {error, Reason :: term()}.
emit_file_location_changed(Location, ExcludedSessions, Offset, OffsetEnd) ->
    event:emit(create_file_location_changed(Location, Offset, OffsetEnd),
        {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends file location changes to all subscribers except for the ones
%% present in 'ExcludedSessions' list. It is faster than execution of
%% emit_file_location_changed on each change.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_locations_changed([{file_ctx:ctx(), non_neg_integer() | undefined,
    non_neg_integer() | undefined}], [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_locations_changed(EventsList, ExcludedSessions) ->
    EventsList2 = lists:map(fun({Location, Offset, OffsetEnd}) ->
        create_file_location_changed(Location, Offset, OffsetEnd)
    end, EventsList),
    event:emit({aggregated, EventsList2}, {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client that permissions of file has changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_perm_changed(file_ctx:ctx()) -> ok | {error, Reason :: term()}.
emit_file_perm_changed(FileCtx) ->
    event:emit(#file_perm_changed_event{
        file_guid = file_ctx:get_guid_const(FileCtx)
    }).

%%--------------------------------------------------------------------
%% @doc
%% Sends event informing subscribed client about file removal.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_removed(file_ctx:ctx(), ExcludedSessions :: [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_removed(FileCtx, ExcludedSessions) ->
    {ParentGuid, _FileCtx2} = file_ctx:get_parent_guid(FileCtx, undefined),
    event:get_subscribers_and_emit(#file_removed_event{file_guid = file_ctx:get_guid_const(FileCtx)},
        ParentGuid, ExcludedSessions).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event informing given client about file rename.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed_to_client(file_ctx:ctx(), file_meta:name(),
    user_ctx:ctx()) -> ok | {error, Reason :: term()}.
emit_file_renamed_to_client(FileCtx, NewName, UserCtx) ->
    SessionId = user_ctx:get_session_id(UserCtx),
    Guid = file_ctx:get_guid_const(FileCtx),
    {ParentGuid, _FileCtx2} = file_ctx:get_parent_guid(FileCtx, UserCtx),
    event:get_subscribers_and_emit(#file_renamed_event{top_entry = #file_renamed_entry{
        old_guid = Guid,
        new_guid = Guid,
        new_parent_guid = ParentGuid,
        new_name = NewName
    }}, ParentGuid, [SessionId]).

%%--------------------------------------------------------------------
%% @doc
%% Sends a list of currently disabled spaces due to exceeded quota.
%% @end
%%--------------------------------------------------------------------
-spec emit_quota_exceeded() -> ok | {error, Reason :: term()}.
emit_quota_exceeded() ->
    case space_quota:get_disabled_spaces() of
        {ok, BlockedSpaces} ->
            ?debug("Sending disabled spaces event ~p", [BlockedSpaces]),
            event:emit(#quota_exceeded_event{spaces = BlockedSpaces});
        {error, _} = Error ->
            ?debug("Cannot send disabled spaces event due to ~p", [Error])
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends event indicating storage helper params have changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_helper_params_changed(StorageId :: storage:id()) ->
    ok | {error, Reason :: term()}.
emit_helper_params_changed(StorageId) ->
    event:emit(#helper_params_changed_event{
        storage_id = StorageId
    }).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates #file_location_changed_event.
%% @end
%%--------------------------------------------------------------------
-spec create_file_location_changed(file_location:record(),
    non_neg_integer() | undefined, non_neg_integer() | undefined) ->
    #file_location_changed_event{}.
create_file_location_changed(Location, Offset, OffsetEnd) ->
    #file_location_changed_event{
        file_location = Location,
        change_beg_offset = Offset, change_end_offset = OffsetEnd
    }.