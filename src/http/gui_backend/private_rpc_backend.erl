%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module implements callback_backend_behaviour.
%%% It is used to handle RPC calls from clients with active session.
%%% @end
%%%-------------------------------------------------------------------
-module(private_rpc_backend).
-behaviour(rpc_backend_behaviour).
-author("Lukasz Opiola").

-include("modules/datastore/datastore_models.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/api_errors.hrl").

%% API
-export([handle/2]).

%%%===================================================================
%%% rpc_backend_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rpc_backend_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(FunctionId :: binary(), RequestData :: term()) ->
    ok | {ok, ResponseData :: term()} | gui_error:error_result().
%%--------------------------------------------------------------------
%% File upload related procedures
%%--------------------------------------------------------------------
handle(<<"fileUploadSuccess">>, Props) ->
    SessionId = gui_session:get_session_id(),
    UploadId = proplists:get_value(<<"uploadId">>, Props),
    ParentId = proplists:get_value(<<"parentId">>, Props),
    FileId = upload_handler:wait_for_file_new_file_id(UploadId),
    upload_handler:upload_map_delete(UploadId),
    {ok, FileHandle} = logical_file_manager:open(
        SessionId, {guid, FileId}, read
    ),
    ok = logical_file_manager:fsync(FileHandle),
    ok = logical_file_manager:release(FileHandle),
    file_data_backend:report_file_upload(FileId, ParentId);

handle(<<"fileUploadFailure">>, Props) ->
    UploadId = proplists:get_value(<<"uploadId">>, Props),
    upload_handler:upload_map_delete(UploadId),
    ok;

handle(<<"fileBatchUploadComplete">>, Props) ->
    ParentId = proplists:get_value(<<"parentId">>, Props),
%%    upload_handler:clean_upload_map(),
    file_data_backend:report_file_batch_complete(ParentId);

% Checks if file can be downloaded (i.e. can be read by the user) and if so,
% returns download URL.
handle(<<"getFileDownloadUrl">>, [{<<"fileId">>, FileId}]) ->
    SessionId = gui_session:get_session_id(),
    case logical_file_manager:check_perms(SessionId, {guid, FileId}, read) of
        {ok, true} ->
            Hostname = gui_ctx:get_requested_hostname(),
            URL = str_utils:format_bin("https://~s/download/~s",
                [Hostname, FileId]),
            {ok, [{<<"fileUrl">>, URL}]};
        {ok, false} ->
            gui_error:report_error(<<"Permission denied">>);
        _ ->
            gui_error:internal_server_error()
    end;

% Checks if file that is displayed in shares view can be downloaded
% (i.e. can be read by the user) and if so, returns download URL.
handle(<<"getSharedFileDownloadUrl">>, [{<<"fileId">>, AssocId}]) ->
    {_, FileId} = op_gui_utils:association_to_ids(AssocId),
    SessionId = gui_session:get_session_id(),
    case logical_file_manager:check_perms(SessionId, {guid, FileId}, read) of
        {ok, true} ->
            Hostname = gui_ctx:get_requested_hostname(),
            URL = str_utils:format_bin("https://~s/download/~s",
                [Hostname, FileId]),
            {ok, [{<<"fileUrl">>, URL}]};
        {ok, false} ->
            gui_error:report_error(<<"Permission denied">>);
        _ ->
            gui_error:internal_server_error()
    end;

%%--------------------------------------------------------------------
%% File manipulation procedures
%%--------------------------------------------------------------------
handle(<<"createFile">>, Props) ->
    SessionId = gui_session:get_session_id(),
    Name = proplists:get_value(<<"fileName">>, Props),
    ParentId = proplists:get_value(<<"parentId">>, Props, null),
    Type = proplists:get_value(<<"type">>, Props),
    case file_data_backend:create_file(SessionId, Name, ParentId, Type) of
        {ok, FileId} ->
            {ok, [{<<"fileId">>, FileId}]};
        Error ->
            Error
    end;

handle(<<"fetchMoreDirChildren">>, Props) ->
    SessionId = gui_session:get_session_id(),
    file_data_backend:fetch_more_dir_children(SessionId, Props);

%%--------------------------------------------------------------------
%% Transfer related procedures
%%--------------------------------------------------------------------

handle(<<"getSpaceTransfers">>, Props) ->
    SpaceId = proplists:get_value(<<"spaceId">>, Props),
    Type = proplists:get_value(<<"type">>, Props),
    StartFromId = proplists:get_value(<<"startFromId">>, Props, undefined),
    Offset = proplists:get_value(<<"offset">>, Props, 0),
    Limit = proplists:get_value(<<"size">>, Props, all),
    transfer_data_backend:list_transfers(SpaceId, Type, StartFromId, Offset, Limit);

handle(<<"getOngoingTransfersForFile">>, Props) ->
    FileGuid = proplists:get_value(<<"fileId">>, Props),
    transfer_data_backend:get_ongoing_transfers_for_file(FileGuid);

%%--------------------------------------------------------------------
%% Space related procedures
%%--------------------------------------------------------------------
handle(<<"getTokenUserJoinSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    SessionId = gui_session:get_session_id(),
    case space_logic:create_user_invite_token(SessionId, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"userJoinSpace">>, [{<<"token">>, Token}]) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case user_logic:join_space(SessionId, UserId, Token) of
        {ok, SpaceId} ->
            gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            SpaceRecord = space_data_backend:space_record(SpaceId),
            SpaceName = proplists:get_value(<<"name">>, SpaceRecord),
            {ok, [{<<"spaceName">>, SpaceName}]};
        ?ERROR_BAD_VALUE_TOKEN(<<"token">>) ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"userLeaveSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case user_logic:leave_space(SessionId, UserId, SpaceId) of
        ok ->
            {ok, AllGroups} = user_logic:get_eff_groups(SessionId, UserId),
            StillHasAccess = lists:any(
                fun(GroupId) ->
                    {ok, EffSpaces} = group_logic:get_eff_spaces(SessionId, GroupId),
                    lists:member(SpaceId, EffSpaces)
                end, AllGroups),
            case StillHasAccess of
                true ->
                    ok;
                false ->
                    gui_async:push_updated(
                        <<"user">>,
                        user_data_backend:user_record(SessionId, UserId)
                    )
            end,
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoinSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    SessionId = gui_session:get_session_id(),
    case space_logic:create_group_invite_token(SessionId, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"groupJoinSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    Token = proplists:get_value(<<"token">>, Props),
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case group_logic:join_space(SessionId, GroupId, Token) of
        {ok, SpaceId} ->
            gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            SpaceRecord = space_data_backend:space_record(SpaceId),
            SpaceName = proplists:get_value(<<"name">>, SpaceRecord),
            {ok, [{<<"spaceName">>, SpaceName}]};
        ?ERROR_BAD_VALUE_TOKEN(<<"token">>) ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"groupLeaveSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    SpaceId = proplists:get_value(<<"spaceId">>, Props),
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case group_logic:leave_space(SessionId, GroupId, SpaceId) of
        ok ->
            {ok, AllGroups} = user_logic:get_eff_groups(SessionId, UserId),
            StillHasAccess = lists:any(
                fun(GrId) ->
                    {ok, EffSpaces} = group_logic:get_eff_spaces(SessionId, GrId),
                    lists:member(SpaceId, EffSpaces)
                end, AllGroups -- [GroupId]),
            case StillHasAccess of
                true ->
                    ok;
                false ->
                    gui_async:push_updated(
                        <<"user">>,
                        user_data_backend:user_record(SessionId, UserId)
                    )
            end,
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenProviderSupportSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    SessionId = gui_session:get_session_id(),
    case space_logic:create_provider_invite_token(SessionId, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end;

handle(<<"createFileShare">>, Props) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    FileId = proplists:get_value(<<"fileId">>, Props),
    Name = proplists:get_value(<<"shareName">>, Props),
    case logical_file_manager:create_share(SessionId, {guid, FileId}, Name) of
        {ok, {ShareId, _}} ->
            gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            % Push file data so GUI knows that is is shared
            {ok, FileData} = file_data_backend:file_record(SessionId, FileId),
            gui_async:push_created(<<"file">>, FileData),
            {ok, [{<<"shareId">>, ShareId}]};
        {error, ?EACCES} ->
            gui_error:report_warning(<<"You do not have permissions to "
            "manage shares in this space.">>);
        _ ->
            gui_error:report_warning(
                <<"Cannot create share due to unknown error.">>)
    end;


%%--------------------------------------------------------------------
%% Group related procedures
%%--------------------------------------------------------------------
handle(<<"getTokenUserJoinGroup">>, [{<<"groupId">>, GroupId}]) ->
    SessionId = gui_session:get_session_id(),
    case group_logic:create_user_invite_token(SessionId, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"userJoinGroup">>, [{<<"token">>, Token}]) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case user_logic:join_group(SessionId, UserId, Token) of
        {ok, GroupId} ->
            gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            GroupRecord = group_data_backend:group_record(GroupId),
            GroupName = proplists:get_value(<<"name">>, GroupRecord),
            {ok, [{<<"groupName">>, GroupName}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot join group due to unknown error.">>)
    end;

handle(<<"userLeaveGroup">>, [{<<"groupId">>, GroupId}]) ->
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case user_logic:leave_group(SessionId, UserId, GroupId) of
        ok ->
            {ok, AllGroups} = user_logic:get_eff_groups(SessionId, UserId),
            StillHasAccess = lists:any(
                fun(GrId) ->
                    {ok, EffChildren} = group_logic:get_eff_children(SessionId, GrId),
                    maps:is_key(GroupId, EffChildren)
                end, AllGroups -- [GroupId]),
            case StillHasAccess of
                true ->
                    ok;
                false ->
                    gui_async:push_updated(
                        <<"user">>,
                        user_data_backend:user_record(SessionId, UserId)
                    )
            end,
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave group due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoinGroup">>, [{<<"groupId">>, GroupId}]) ->
    SessionId = gui_session:get_session_id(),
    case group_logic:create_group_invite_token(SessionId, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"groupJoinGroup">>, Props) ->
    ChildGroupId = proplists:get_value(<<"groupId">>, Props),
    Token = proplists:get_value(<<"token">>, Props),
    SessionId = gui_session:get_session_id(),
    UserId = gui_session:get_user_id(),
    case group_logic:join_group(SessionId, ChildGroupId, Token) of
        {ok, ParentGroupId} ->
            gui_async:push_updated(
                <<"user">>,
                user_data_backend:user_record(SessionId, UserId)
            ),
            ChildGroupRecord = group_data_backend:group_record(ChildGroupId),
            gui_async:push_updated(<<"group">>, ChildGroupRecord),
            ParentGroupRecord = group_data_backend:group_record(ParentGroupId),
            gui_async:push_updated(<<"group">>, ParentGroupRecord),
            PrntGroupName = proplists:get_value(<<"name">>, ParentGroupRecord),
            {ok, [{<<"groupName">>, PrntGroupName}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot join group due to unknown error.">>)
    end;

handle(<<"groupLeaveGroup">>, Props) ->
    ParentGroupId = proplists:get_value(<<"parentGroupId">>, Props),
    ChildGroupId = proplists:get_value(<<"childGroupId">>, Props),
    SessionId = gui_session:get_session_id(),
    case group_logic:leave_group(SessionId, ChildGroupId, ParentGroupId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave group due to unknown error.">>)
    end;

handle(<<"getTokenRequestSpaceCreation">>, [{<<"groupId">>, _GroupId}]) ->
    gui_error:report_error(
        <<"This feature is no longer supported.">>);

handle(_, _) ->
    gui_error:report_error(<<"Not implemented">>).
