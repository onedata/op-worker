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
-author("Lukasz Opiola").
-behaviour(rpc_backend_behaviour).

-include_lib("ctool/include/logging.hrl").
-include_lib("ctool/include/oz/oz_spaces.hrl").

%% API
-export([handle/2]).


%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link rpc_backend_behaviour} callback handle/2.
%% @end
%%--------------------------------------------------------------------
-spec handle(FunctionId :: binary(), RequestData :: term()) ->
    ok | {ok, ResponseData :: term()} | gui_error:error_result().
handle(<<"fileUploadComplete">>, [{<<"fileId">>, FileId}]) ->
    upload_handler:upload_map_delete(FileId),
    ok;

handle(<<"userJoinSpace">>, [{<<"token">>, Token}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:join_space(UserAuth, Token) of
        {ok, SpaceId} ->
            {ok, #space_details{
                name = SpaceName
            }} = oz_spaces:get_details(UserAuth, SpaceId),
            {ok, SpaceName};
        {error, invalid_token_value} ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"userLeaveSpace">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:leave_space(UserAuth, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenUserJoin">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:get_invite_user_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoin">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:get_invite_group_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"getTokenProviderSupport">>, [{<<"spaceId">>, SpaceId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case space_logic:get_invite_provider_token(UserAuth, SpaceId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end;

handle(<<"userLeaveGroup">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:leave_group(UserAuth, GroupId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end;

handle(<<"getTokenGroupJoin">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:get_invite_user_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite user token due to unknown error.">>)
    end;

handle(<<"getTokenUserJoin">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:get_invite_group_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite group token due to unknown error.">>)
    end;

handle(<<"getTokenRequestSpaceCreation">>, [{<<"groupId">>, GroupId}]) ->
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:get_create_space_token(UserAuth, GroupId) of
        {ok, Token} ->
            {ok, Token};
        {error, _} ->
            gui_error:report_error(
                <<"Cannot get invite provider token due to unknown error.">>)
    end;

handle(<<"groupJoinSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    Token = proplists:get_value(<<"token">>, Props),
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:join_space(UserAuth, GroupId, Token) of
        {ok, SpaceId} ->
            {ok, #space_details{
                name = SpaceName
            }} = oz_spaces:get_details(UserAuth, SpaceId),
            {ok, SpaceName};
        {error, invalid_token_value} ->
            gui_error:report_warning(<<"Invalid token value.">>)
    end;

handle(<<"groupLeaveSpace">>, Props) ->
    GroupId = proplists:get_value(<<"groupId">>, Props),
    SpaceId = proplists:get_value(<<"spaceId">>, Props),
    UserAuth = op_gui_utils:get_user_rest_auth(),
    case group_logic:leave_space(UserAuth, GroupId, SpaceId) of
        ok ->
            ok;
        {error, _} ->
            gui_error:report_error(
                <<"Cannot leave space due to unknown error.">>)
    end.
