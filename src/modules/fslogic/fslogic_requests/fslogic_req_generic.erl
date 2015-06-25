%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc FSLogic generic (both for regular and special files) request handlers.
%%% @end
%%%-------------------------------------------------------------------
-module(fslogic_req_generic).
-author("Rafal Slota").

-include("proto/oneclient/fuse_messages.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([chmod/3, get_file_attr/2, delete_file/2, rename_file/3]).

%% @todo: uncomment 'check_permissions' annotations after implementing
%%        methods below. Annotations have to be commented out due to dizlyzer errors.

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc Changes file permissions.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------

-spec chmod(fslogic:ctx(), File :: fslogic:file(), Perms :: fslogic:posix_permissions()) ->
    no_return().
%%-check_permissions({owner, 2}).
chmod(_, _File, _Mode) ->
    ?NOT_IMPLEMENTED.

%%--------------------------------------------------------------------
%% @doc Gets file's attributes.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(Ctx :: fslogic:ctx(), File :: fslogic:file()) ->
    FuseResponse :: fuse_response().
get_file_attr(Ctx, {path, Path}) ->
    {ok, Tokens} = fslogic_path:verify_file_path(Path),
    CanonicalFileEntry = fslogic_path:get_canonical_file_entry(Ctx, Tokens),
    get_file_attr(CanonicalFileEntry);
get_file_attr(_Ctx, File) ->
    get_file_attr(File).

%%--------------------------------------------------------------------
%% @doc Deletes file.
%% For best performance use following arg types: document -> uuid -> path
%% @end
%%--------------------------------------------------------------------
-spec delete_file(fslogic:ctx(), File :: fslogic:file()) ->
    FuseResponse :: fuse_response().
%%-check_permissions({write, {parent, 2}}).
delete_file(_, File) ->
    {ok, #document{value = #file_meta{type = Type}} = FileDoc} = file_meta:get(File),
    {ok, FileChildren} = case Type of
                             ?DIRECTORY_TYPE ->
                                 file_meta:list_children(FileDoc, 0, 1);
                             _ ->
                                 {ok, []}
                         end,
    case length(FileChildren) of
        0 ->
            ok = file_meta:delete(FileDoc),
            {ok, ParentDoc} = file_meta:get_parent(FileDoc),
            {ok, _} = file_meta:update(ParentDoc, #{mtime => utils:time()}),
            #fuse_response{status = #status{code = ?OK}};
        _ ->
            #fuse_response{status = #status{code = ?ENOTEMPTY}}
    end.

%%--------------------------------------------------------------------
%% @doc Renames file.
%% For best performance use following arg types: path -> uuid -> document
%% @end
%%--------------------------------------------------------------------
-spec rename_file(fslogic:ctx(), SourcePath :: fslogic:file(), TargetPath :: file_meta:path()) ->
    no_return().
%%-check_permissions([{write, {parent, {path, 2}}}, {write, {parent, {path, 3}}}]).
rename_file(_, _SourcePath, _TargetPath) ->
    ?NOT_IMPLEMENTED.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Utility function returning file attributes.
%% @end
%%--------------------------------------------------------------------
-spec get_file_attr(File :: fslogic:file()) -> FuseResponse :: fuse_response().
get_file_attr(File) ->
    case file_meta:get(File) of
        {ok, #document{key = UUID, value = #file_meta{
            type = Type, mode = Mode, atime = ATime, mtime = MTime,
            ctime = CTime, uid = UID, size = Size, name = Name
        }}} -> #fuse_response{status = #status{code = ?OK}, fuse_response =
        #file_attr{
            uuid = UUID, type = Type, mode = Mode, atime = ATime, mtime = MTime,
            ctime = CTime, uid = UID, size = Size, name = Name
        }};
        {error, {not_found, _}} ->
            #fuse_response{status = #status{code = ?ENOENT}}
    end.