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
-module(fslogic_event).
-author("Krzysztof Trzepla").

-include("modules/events/definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([emit_file_attr_update/2, emit_file_sizeless_attrs_update/1,
    emit_file_location_update/2, emit_permission_changed/1, emit_file_removal/1]).
-export([emit_quota_exeeded/0]).

%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Sends list of currently disabled spaces due to exeeded quota.
%% @end
%%--------------------------------------------------------------------
-spec emit_quota_exeeded() ->
    ok | {error, Reason :: term()}.
emit_quota_exeeded() ->
    BlockedSpaces = space_quota:get_disabled_spaces(),
    ?debug("Sending disabled spaces ~p", [BlockedSpaces]),
    event:emit(#event{object = #quota_exeeded_event{spaces = BlockedSpaces}}).


%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_attr_update(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_attr_update(FileEntry, ExcludedSessions) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),
    FileGUID = fslogic_uuid:to_file_guid(FileUUID),
    case logical_file_manager:stat(?ROOT_SESS_ID, {guid, FileGUID}) of
        {ok, #file_attr{} = FileAttr} ->
            ?debug("Sending new attributes for file ~p to all sessions except ~p",
                [FileEntry, ExcludedSessions]),
            event:emit(#event{object = #update_event{object = FileAttr}}, {exclude, ExcludedSessions});
        {error, Reason} ->
            ?error("Unable to get new attributes for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends current file attributes excluding size to all subscribers.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_sizeless_attrs_update(fslogic_worker:file()) ->
    ok | {error, Reason :: term()}.
emit_file_sizeless_attrs_update(FileEntry) ->
    {ok, FileUUID} = file_meta:to_uuid(FileEntry),
    FileGUID = fslogic_uuid:to_file_guid(FileUUID),
    case logical_file_manager:stat(?ROOT_SESS_ID, {guid, FileGUID}) of
        {ok, #file_attr{} = FileAttr} ->
            ?debug("Sending new times for file ~p to all subscribers", [FileEntry]),
            SizelessFileAttr = FileAttr#file_attr{size = undefined},
            event:emit(#event{object = #update_event{object = SizelessFileAttr}});
        {error, Reason} ->
            ?error("Unable to get new times for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Sends current file location to all subscribers except for the ones present
%% in 'ExcludedSessions' list.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_location_update(fslogic_worker:file(), [session:id()]) ->
    ok | {error, Reason :: term()}.
emit_file_location_update(FileEntry, ExcludedSessions) ->
    try
        {ok, #document{} = File} = file_meta:get(FileEntry),
        #document{value = #file_location{uuid = FileUuid} = FileLocation} = fslogic_utils:get_local_file_location(File),
        event:emit(#event{object = #update_event{object = file_location:ensure_blocks_not_empty(FileLocation#file_location{uuid = fslogic_uuid:to_file_guid(FileUuid)})}},
            {exclude, ExcludedSessions})
    catch
        _:Reason ->
            ?error_stacktrace("Unable to push new location for file ~p due to: ~p", [FileEntry, Reason]),
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client that permissions of file has changed.
%% @end
%%--------------------------------------------------------------------
-spec emit_permission_changed(FileUuid :: file_meta:uuid()) ->
    ok | {error, Reason :: term()}.
emit_permission_changed(FileUuid) ->
    event:emit(#event{object = #permission_changed_event{file_uuid = fslogic_uuid:to_file_guid(FileUuid)}}).

%%--------------------------------------------------------------------
%% @doc
%% Send event informing subscribed client about file removal.
%% @end
%%--------------------------------------------------------------------
-spec emit_file_removal(FileGUID :: fslogic_worker:file_guid()) ->
    ok | {error, Reason :: term()}.
emit_file_removal(FileGUID) ->
    event:emit(#event{object = #file_removal_event{file_uuid = FileGUID}}).

%%%===================================================================
%%% Internal functions
%%%===================================================================