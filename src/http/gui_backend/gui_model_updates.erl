%%%-------------------------------------------------------------------
%%% @author Lukasz Opiola
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @todo (VFS-1865) Temporal solution for GUI push updates
%%% This module includes utility functions used in gui modules.
%%% @end
%%%-------------------------------------------------------------------
-module(gui_model_updates).
-author("Lukasz Opiola").

-include("proto/common/credentials.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include("global_definitions.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([changed/2, deleted/2]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @todo (VFS-1865) Temporal solution for GUI push updates
%% Processes model changes and informs Ember client about changes.
%% @end
%%--------------------------------------------------------------------
-spec changed(Model :: atom(), UUID :: binary()) -> ok.
changed(Model, UUID) ->
    ?alert("CHANGED\t(~p)\t~s", [Model, UUID]),
    case Model of
        onedata_user ->
            changed_user(UUID);
        space_info ->
            changed_space(UUID);
        file_meta ->
            changed_file(UUID)
    end.


%%--------------------------------------------------------------------
%% @doc
%% @todo (VFS-1865) Temporal solution for GUI push updates
%% Processes model changes and informs Ember client about changes.
%% @end
%%--------------------------------------------------------------------
-spec deleted(Model :: atom(), UUID :: binary()) -> ok.
deleted(Model, UUID) ->
    ?alert("DELETED\t(~p)\t~s", [Model, UUID]),
    case Model of
        onedata_user ->
            deleted_user(UUID);
        space_info ->
            deleted_space(UUID);
        file_meta ->
            deleted_file(UUID)
    end.


changed_file(FileId) ->
    lists:foreach(
        fun({SessionId, Pids}) ->
            try
                {ok, Data} = file_data_backend:file_record(SessionId, FileId),
                lists:foreach(
                    fun(Pid) ->
                        gui_async:push_created(<<"file">>, Data, Pid)
                    end, Pids)
            catch T:M ->
                ?dump({changed_file, T, M, erlang:get_stacktrace()})
            end
        end, op_gui_utils:get_all_backend_pids(file_data_backend)).


deleted_file(FileId) ->
    lists:foreach(
        fun({_SessionId, Pids}) ->
            try
                lists:foreach(
                    fun(Pid) ->
                        gui_async:push_deleted(<<"file">>, FileId, Pid)
                    end, Pids)
            catch T:M ->
                ?dump({deleted_file, T, M, erlang:get_stacktrace()})
            end
        end, op_gui_utils:get_all_backend_pids(file_data_backend)).


changed_user(UserId) ->
    ok.


deleted_user(UserId) ->
    ok.


changed_space(UUID) ->
    {ok, #document{
        value = #space_info{
            id = GlobalSpaceId
        }}} = space_info:get(UUID),
    SpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(GlobalSpaceId),

    case UUID of
        GlobalSpaceId ->
            % Global space doc changed - update 'space' models
            lists:foreach(
                fun({SessionId, Pids}) ->
                    try
                        g_session:set_session_id(SessionId),
                        {ok, [SpaceData]} = space_data_backend:find(<<"space">>, [SpaceDirId]),
                        UsersPerms = proplists:get_value(<<"userPermissions">>, SpaceData),
                        {ok, UsersPermsData} = space_data_backend:find(<<"space-user-permission">>, UsersPerms),
                        GroupsPerms = proplists:get_value(<<"groupPermissions">>, SpaceData),
                        {ok, GroupsPermsData} = space_data_backend:find(<<"space-group-permission">>, GroupsPerms),
                        lists:foreach(
                            fun(Pid) ->
                                gui_async:push_updated(<<"space">>, SpaceData, Pid),
                                gui_async:push_updated(<<"space-user-permission">>, UsersPermsData, Pid),
                                gui_async:push_updated(<<"space-group-permission">>, GroupsPermsData, Pid)
                            end, Pids)
                    catch T:M ->
                        ?dump({changed_space, T, M, erlang:get_stacktrace()})
                    end
                end, op_gui_utils:get_all_backend_pids(space_data_backend));
        _ ->
            ok
    end,
    % Always update 'data-space' models
    lists:foreach(
        fun({SessionId, Pids}) ->
            try
                g_session:set_session_id(SessionId),
                % Returns error when this space is not supported by this provider
                case data_space_data_backend:find(<<"data-space">>, [SpaceDirId]) of
                    {ok, SpaceData} ->
                        lists:foreach(
                            fun(Pid) ->
                                gui_async:push_updated(<<"data-space">>, SpaceData, Pid)
                            end, Pids);
                    _ ->
                        ok
                end
            catch T:M ->
                ?dump({changed_space, T, M, erlang:get_stacktrace()})
            end
        end, op_gui_utils:get_all_backend_pids(data_space_data_backend)).


deleted_space(UUID) ->
    {ok, #document{
        value = #space_info{
            id = GlobalSpaceId
        }}} = space_info:get(UUID),
    SpaceDirId = fslogic_uuid:spaceid_to_space_dir_uuid(GlobalSpaceId),

    case UUID of
        GlobalSpaceId ->
            % Global space doc changed - update 'space' models
            lists:foreach(
                fun({_SessionId, Pids}) ->
                    try
                        lists:foreach(
                            fun(Pid) ->
                                gui_async:push_deleted(<<"space">>, SpaceDirId, Pid)
                            end, Pids)
                    catch T:M ->
                        ?dump({deleted_space, T, M, erlang:get_stacktrace()})
                    end
                end, op_gui_utils:get_all_backend_pids(file_data_backend));
        _ ->
            ok
    end,
    % Always update 'data-space' models
    lists:foreach(
        fun({_SessionId, Pids}) ->
            try
                lists:foreach(
                    fun(Pid) ->
                        gui_async:push_deleted(<<"data-space">>, SpaceDirId, Pid)
                    end, Pids)
            catch T:M ->
                ?dump({deleted_space, T, M, erlang:get_stacktrace()})
            end
        end, op_gui_utils:get_all_backend_pids(file_data_backend)).
