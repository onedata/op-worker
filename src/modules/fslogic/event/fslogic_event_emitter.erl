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
-export([emit_file_attr_changed/2, emit_sizeless_file_attrs_changed/1,
    emit_file_location_changed/2, emit_file_location_changed/3,
    emit_file_perm_changed/1, emit_file_removed/2, emit_file_renamed/3,
    emit_file_renamed_to_client/3, emit_quota_exeeded/0]).

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
    #fuse_response{fuse_response = #file_attr{} = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx, true),
    event:emit(#file_attr_changed_event{file_attr = FileAttr},
        {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes excluding size to all subscribers.
%% @end
%%--------------------------------------------------------------------
-spec emit_sizeless_file_attrs_changed(file_ctx:ctx()) ->
    ok | {error, Reason :: term()}.
emit_sizeless_file_attrs_changed(FileCtx) ->
    #fuse_response{fuse_response = #file_attr{} = FileAttr} =
        attr_req:get_file_attr_insecure(user_ctx:new(?ROOT_SESS_ID), FileCtx, true),
    event:emit(#file_attr_changed_event{
        file_attr = FileAttr#file_attr{size = undefined}
    }).

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
    fslogic_blocks:block() | undefined) -> ok | {error, Reason :: term()}.
emit_file_location_changed(FileCtx, ExcludedSessions, Range) ->
    {Location, _FileCtx2} = file_ctx:get_file_location_with_filled_gaps(FileCtx, Range),
    event:emit(#file_location_changed_event{
        file_location = Location
    }, {exclude, ExcludedSessions}).

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
    event:emit(#file_removed_event{file_guid = file_ctx:get_guid_const(FileCtx)},
        {exclude, ExcludedSessions}).

%%--------------------------------------------------------------------
%% @doc
%% Sends an event informing subscribed client about file rename and guid change.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_renamed(TopEntry :: #file_renamed_entry{},
    ChildEntries :: [#file_renamed_entry{}], ExcludedSessions :: [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_renamed(TopEntry, ChildEntries, ExcludedSessions) ->
    event:emit(#file_renamed_event{
        top_entry = TopEntry, child_entries = ChildEntries
    }, {exclude, ExcludedSessions}).

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
    event:emit(#file_renamed_event{top_entry = #file_renamed_entry{
        old_guid = Guid,
        new_guid = Guid,
        new_parent_guid = ParentGuid,
        new_name = NewName
    }}, SessionId).

%%--------------------------------------------------------------------
%% @doc
%% Sends a list of currently disabled spaces due to exeeded quota.
%% @end
%%--------------------------------------------------------------------
-spec emit_quota_exeeded() -> ok | {error, Reason :: term()}.
emit_quota_exeeded() ->
    BlockedSpaces = space_quota:get_disabled_spaces(),
    ?debug("Sending disabled spaces ~p", [BlockedSpaces]),
    event:emit(#quota_exceeded_event{spaces = BlockedSpaces}).