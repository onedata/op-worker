%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on directories.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([mkdir/4, read_dir/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?add_subcontainer, 2}, {?traverse_container, 2}]).
mkdir(UserCtx, ParentFileCtx, Name, Mode) ->
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?DIRECTORY_TYPE,
        mode = Mode,
        uid = user_ctx:get_user_id(UserCtx)
    }},
    {ParentDoc = #document{key = ParentUuid}, ParentFileCtx2} = file_ctx:get_file_doc(ParentFileCtx),
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx2),
    {ok, DirUuid} = file_meta:create(ParentDoc, File), %todo maybe pass file_ctx inside
    {ok, _} = times:create(#document{key = DirUuid, value = #times{mtime = CTime, atime = CTime, ctime = CTime}}),
    fslogic_times:update_mtime_ctime({uuid, ParentUuid}, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #dir{uuid = fslogic_uuid:uuid_to_guid(DirUuid, SpaceId)}
    }.

%%--------------------------------------------------------------------
%% @doc
%% Lists directory. Starts with ROffset entity and limits returned list to RCount size.
%% @end
%%--------------------------------------------------------------------
-spec read_dir(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: non_neg_integer(), Limit :: non_neg_integer()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?list_container, 2}]).
read_dir(UserCtx, FileCtx, Offset, Limit) ->
    {FileDoc, FileCtx2} = file_ctx:get_file_doc(FileCtx),
    {Children, _FileCtx3} = file_ctx:get_file_children(FileCtx2, UserCtx, Offset, Limit),
    ChildrenLinks =
        lists:map(fun(ChildFile) ->
            ChildGuid = file_ctx:get_guid_const(ChildFile),
            {ChildName, _ChildFile3} = file_ctx:get_aliased_name(ChildFile, UserCtx),
            #child_link{name = ChildName, uuid = ChildGuid}
        end, Children),
    fslogic_times:update_atime(FileDoc, user_ctx:get_user_id(UserCtx)), %todo pass file_ctx
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children{
            child_links = ChildrenLinks
        }
    }.

%%%===================================================================
%%% Internal functions
%%%===================================================================
