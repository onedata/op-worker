%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on directories.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").

%% API
-export([mkdir/4, read_dir/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv mkdir_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec mkdir(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    fslogic_worker:fuse_response().
mkdir(UserCtx, ParentFileCtx, Name, Mode) ->
    check_permissions:execute(
        [traverse_ancestors, ?traverse_container, ?add_subcontainer],
        [UserCtx, ParentFileCtx, Name, Mode],
        fun mkdir_insecure/4).

%%--------------------------------------------------------------------
%% @equiv read_dir_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec read_dir(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: non_neg_integer(), Limit :: non_neg_integer()) ->
    fslogic_worker:fuse_response().
read_dir(UserCtx, FileCtx, Offset, Limit) ->
    check_permissions:execute(
        [traverse_ancestors, ?list_container],
        [UserCtx, FileCtx, Offset, Limit],
        fun read_dir_insecure/4).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir_insecure(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    fslogic_worker:fuse_response().
mkdir_insecure(UserCtx, ParentFileCtx, Name, Mode) ->
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?DIRECTORY_TYPE,
        mode = Mode,
        owner = user_ctx:get_user_id(UserCtx)
    }},
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    {ok, DirUuid} = file_meta:create({uuid, ParentUuid}, File), %todo maybe pass file_ctx inside
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx),
    {ok, _} = times:save_new(#document{
        key = DirUuid,
        value = #times{mtime = CTime, atime = CTime, ctime = CTime},
        scope = SpaceId
    }),
    fslogic_times:update_mtime_ctime(ParentFileCtx),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #dir{guid = fslogic_uuid:uuid_to_guid(DirUuid, SpaceId)}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Lists directory. Starts with ROffset entity and limits returned list to RCount size.
%% @end
%%--------------------------------------------------------------------
-spec read_dir_insecure(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: non_neg_integer(), Limit :: non_neg_integer()) ->
    fslogic_worker:fuse_response().
read_dir_insecure(UserCtx, FileCtx, Offset, Limit) ->
    {Children, FileCtx2} = file_ctx:get_file_children(FileCtx, UserCtx, Offset, Limit),
    ChildrenLinks =
        lists:map(fun(ChildFile) ->
            ChildGuid = file_ctx:get_guid_const(ChildFile),
            {ChildName, _ChildFile3} = file_ctx:get_aliased_name(ChildFile, UserCtx),
            #child_link{name = ChildName, guid = ChildGuid}
        end, Children),
    fslogic_times:update_atime(FileCtx2),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children{
            child_links = ChildrenLinks
        }
    }.