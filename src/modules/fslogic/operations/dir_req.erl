%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Requests operating on directories
%%% @end
%%%--------------------------------------------------------------------
-module(dir_req).
-author("Tomasz Lichon").

-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("annotations/include/annotations.hrl").

%% API
-export([mkdir/4]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(fslogic_context:ctx(), ParentFile :: file_info:file_info(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    fslogic_worker:fuse_response().
-check_permissions([{traverse_ancestors, 2}, {?add_subcontainer, 2}, {?traverse_container, 2}]).
mkdir(Ctx, ParentFile, Name, Mode) ->
    CTime = erlang:system_time(seconds),
    File = #document{value = #file_meta{
        name = Name,
        type = ?DIRECTORY_TYPE,
        mode = Mode,
        uid = fslogic_context:get_user_id(Ctx)
    }},
    {ParentDoc = #document{key = ParentUuid}, ParentFile2} = file_info:get_file_doc(ParentFile),
    SpaceId = file_info:get_space_id(ParentFile2),
    case file_meta:create(ParentDoc, File) of %todo maybe pass file_info inside
        {ok, DirUUID} ->
            {ok, _} = times:create(#document{key = DirUUID, value = #times{mtime = CTime, atime = CTime, ctime = CTime}}),
            fslogic_times:update_mtime_ctime({uuid, ParentUuid}, fslogic_context:get_user_id(Ctx)), %todo pass file_info
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #dir{uuid = fslogic_uuid:uuid_to_guid(DirUUID, SpaceId)}
            };
        {error, already_exists} ->
            #fuse_response{status = #status{code = ?EEXIST}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
