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

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/posix/errors.hrl").

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
%% Space related procedures
%%--------------------------------------------------------------------
handle(<<"getTokenUserJoinSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:get_invite_user_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"userJoinSpace">>, [{<<"token">>, Token}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    case space_logic:join_space(UserAuth, Token) of
        {ok, SpaceId} ->
            user_data_backend:push_modified_user(
                UserAuth, UserId, <<"spaces">>, add, SpaceId
            ),
            SpaceRecord = space_data_backend:space_record(SpaceId),
            SpaceName = proplists:get_value(<<"name">>, SpaceRecord),
            {ok, [{<<"spaceName">>, SpaceName}]};
        {error, invalid_token_value} ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"userLeaveSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    case space_logic:leave_space(UserAuth, SpaceId) of
        ok ->
            AllGroups = op_gui_utils:find_all_groups(UserAuth, UserId),
            StillHasAccess = lists:any(
                fun(GroupId) ->
                    {ok, #document{
                        value = #od_group{spaces = Spaces}}
                    } = od_group:get(GroupId),
                    lists:member(SpaceId, Spaces)
                end, AllGroups),
            case StillHasAccess of
                true ->
                    ok;
                false ->
                    user_data_backend:push_modified_user(
                        UserAuth, UserId, <<"spaces">>, remove, SpaceId
                    )
            end,
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoinSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:get_invite_group_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"groupJoinSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    Token = proplists:get_value(<<"token">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    case group_logic:join_space(UserAuth, GroupId, Token) of
        {ok, SpaceId} ->
            user_data_backend:push_modified_user(
                UserAuth, UserId, <<"spaces">>, add, SpaceId
            ),
            SpaceRecord = space_data_backend:space_record(SpaceId),
            SpaceName = proplists:get_value(<<"name">>, SpaceRecord),
            {ok, [{<<"spaceName">>, SpaceName}]};
        {error, invalid_token_value} ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"groupLeaveSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    SpaceId = proplists:get_value(<<"spaceId">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    case group_logic:leave_space(UserAuth, GroupId, SpaceId) of
        ok ->
            AllGroups = op_gui_utils:find_all_groups(UserAuth, UserId),
            StillHasAccess = lists:any(
                fun(GrId) ->
                    {ok, #document{
                        value = #od_group{spaces = Spaces}}
                    } = od_group:get(GrId),
                    lists:member(SpaceId, Spaces)
                end, AllGroups -- [GroupId]),
            case StillHasAccess of
                true ->
                    ok;
                false ->
                    user_data_backend:push_modified_user(
                        UserAuth, UserId, <<"spaces">>, remove, SpaceId
                    )
            end,
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenProviderSupportSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case space_logic:get_invite_provider_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end;

handle(<<"createFileShare">>, Props) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    SessionId = gui_session:get_session_id(),
    FileId = proplists:get_value(<<"fileId">>, Props),
    Name = proplists:get_value(<<"shareName">>, Props),
    case logical_file_manager:create_share(SessionId, {guid, FileId}, Name) of
        {ok, {ShareId, _}} ->
            user_data_backend:push_modified_user(
                UserAuth, UserId, <<"shares">>, add, ShareId
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
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:get_invite_user_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"userJoinGroup">>, [{<<"token">>, Token}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    case user_logic:join_group(UserAuth, Token) of
        {ok, GroupId} ->
            user_data_backend:push_modified_user(
                UserAuth, UserId, <<"groups">>, add, GroupId
            ),
            GroupRecord = group_data_backend:group_record(GroupId),
            GroupName = proplists:get_value(<<"name">>, GroupRecord),
            {ok, [{<<"groupName">>, GroupName}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot join group due to unknown error.">>)
    end;

handle(<<"userLeaveGroup">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    case user_logic:leave_group(UserAuth, GroupId) of
        ok ->
            AllGroups = op_gui_utils:find_all_groups(UserAuth, UserId),
            StillHasAccess = lists:any(
                fun(GrId) ->
                    {ok, #document{
                        value = #od_group{children = Children}}
                    } = od_group:get(GrId),
                    proplists:is_defined(GroupId, Children)
                end, AllGroups -- [GroupId]),
            case StillHasAccess of
                true ->
                    ok;
                false ->
                    user_data_backend:push_modified_user(
                        UserAuth, UserId, <<"groups">>, remove, GroupId
                    )
            end,
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave group due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoinGroup">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:get_invite_group_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"groupJoinGroup">>, Props) ->
    ChildGroupId = proplists:get_value(<<"groupId">>, Props),
    Token = proplists:get_value(<<"token">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    UserId = gui_session:get_user_id(),
    case group_logic:join_group(UserAuth, ChildGroupId, Token) of
        {ok, ParentGroupId} ->
            user_data_backend:push_modified_user(
                UserAuth, UserId, <<"groups">>, add, ParentGroupId
            ),
            ChildGroupRecord = group_data_backend:group_record(ChildGroupId),
            gui_async:push_updated(<<"group">>, ChildGroupRecord),
            ParentGroupRecord = group_data_backend:group_record(ParentGroupId),
            PrntGroupName = proplists:get_value(<<"name">>, ParentGroupRecord),
            {ok, [{<<"groupName">>, PrntGroupName}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot join group due to unknown error.">>)
    end;

handle(<<"groupLeaveGroup">>, Props) ->
    ParentGroupId = proplists:get_value(<<"parentGroupId">>, Props),
    ChildGroupId = proplists:get_value(<<"childGroupId">>, Props),
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:leave_group(UserAuth, ParentGroupId, ChildGroupId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave group due to unknown error.">>)
    end;

handle(<<"getTokenRequestSpaceCreation">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_auth(),
    case group_logic:get_create_space_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, [{<<"token">>, Token}]};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end;

handle(_, _) ->
    gui_error:report_error(<<"Not implemented">>).
