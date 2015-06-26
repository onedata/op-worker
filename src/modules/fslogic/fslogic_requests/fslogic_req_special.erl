%% ===================================================================
%% @author Rafal Slota
%% @copyright (C): 2013, ACK CYFRONET AGH
%% This software is released under the MIT license
%% cited in 'LICENSE.txt'.
%% @end
%% ===================================================================
%% @doc: FSLogic request handlers for special files.
%% @end
%% ===================================================================
-module(fslogic_req_special).
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([mkdir/4, read_dir/4, link/3, read_link/2]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir(Ctx :: fslogic:ctx(), ParentUUID :: file_meta:uuid(),
    Name :: file_meta:name(), Mode :: non_neg_integer()) ->
    FuseResponse :: fuse_response().
mkdir(#fslogic_ctx{session = #session{identity = #identity{user_id = UUID}}} = Ctx,
    UUID, Name, Mode) ->
    {ok, #document{key = DefaultSpaceUUID}} = fslogic_spaces:get_default_space(Ctx),
    mkdir(Ctx, DefaultSpaceUUID, Name, Mode);
mkdir(Ctx, ParentUUID, Name, Mode) ->
    CTime = utils:time(),
    File = #document{value = #file_meta{
        name = Name,
        type = ?DIRECTORY_TYPE,
        mode = Mode,
        mtime = CTime,
        atime = CTime,
        ctime = CTime,
        uid = fslogic_context:get_user_id(Ctx)
    }},
    case file_meta:create({uuid, ParentUUID}, File) of
        {ok, _} ->
            {ok, _} = file_meta:update({uuid, ParentUUID}, #{mtime => CTime}),
            #fuse_response{status = #status{code = ?OK}};
        {error, already_exists} ->
            #fuse_response{status = #status{code = ?EEXIST}}
    end.

%%--------------------------------------------------------------------
%% @doc Lists directory. Start with ROffset entity and limit returned list to RCount size.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec read_dir(Ctx :: fslogic:ctx(), File :: fslogic:file(),
    Offset :: file_meta:offset(), Count :: file_meta:size()) ->
    FuseResponse :: fuse_response().
read_dir(Ctx, File, Offset, Size) ->
    UserId = fslogic_context:get_user_id(Ctx),
    {ok, #document{key = Key} = FileDoc} = file_meta:get(File),
    {ok, ChildLinks} = file_meta:list_children(FileDoc, Offset, Size),
    case Key of
        UserId ->
            {ok, DefaultSpace} = fslogic_spaces:get_default_space(Ctx),
            {ok, DefaultSpaceChildLinks} =
                case Offset of
                    0 -> file_meta:list_children(DefaultSpace, Offset, Size - 1);
                    _ -> file_meta:list_children(DefaultSpace, Offset - 1, Size)
                end,
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #file_children{
                    child_links = ChildLinks ++ DefaultSpaceChildLinks
                }
            };
        _ ->
            #fuse_response{status = #status{code = ?OK},
                fuse_response = #file_children{child_links = ChildLinks}}
    end.

%%--------------------------------------------------------------------
%% @doc Creates new symbolic link.
%% @end
%%--------------------------------------------------------------------
-spec link(fslogic:ctx(), Path :: file_meta:path(), LinkValue :: binary()) ->
    no_return().
link(_, _File, _LinkValue) ->
    ?NOT_IMPLEMENTED.

%%--------------------------------------------------------------------
%% @doc Gets value of symbolic link.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec read_link(fslogic:ctx(), File :: fslogic:file()) ->
    no_return().
read_link(_, _File) ->
    ?NOT_IMPLEMENTED.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
