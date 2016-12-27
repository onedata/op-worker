%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Opaque type storing informations about file.
%%% @end
%%%--------------------------------------------------------------------
-module(file_info).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/posix/errors.hrl").

%% Record definition
-record(file_info, {
    path :: undefined | path(),
    guid :: undefined | guid(),
    space_dir_doc :: undefined | file_meta:doc(),
    file_doc :: undefined | file_meta:doc() | {error, term()},
    parent :: undefined | file_info(),
    storage_file_id :: undefined | helpers:file()
}).

-type path() :: file_meta:path().
-type guid() :: fslogic_worker:file_guid().
-type file_info() :: #file_info{}.

%% API
-export([new_by_path/2, new_by_guid/1]).
-export([get_share_id/1, get_path/1, get_space_id/1, get_space_dir_uuid/1,
    get_guid/1, get_file_doc/1, get_parent/1, get_storage_file_id/1]).
-export([is_file_info/1, is_space_dir/1, is_user_root_dir/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Create new file_info using file's path
%% @end
%%--------------------------------------------------------------------
-spec new_by_path(fslogic_context:ctx(), path()) -> {NewCtx :: fslogic_context:ctx(), file_info()}.
new_by_path(Ctx, Path) ->
    {ok, Tokens} = fslogic_path:tokenize_skipping_dots(Path),
    case session:is_special(fslogic_context:get_session_id(Ctx)) of
        true ->
            throw({invalid_request, <<"Path resolution requested in the context of root or guest session. You may only operate on guids in this context.">>});
        false ->
            case Tokens of
                [<<"/">>] ->
                    {UserRootDirUuid, NewCtx} = fslogic_context:get_user_root_dir_uuid(Ctx),
                    UserRootDirGuid = fslogic_uuid:user_root_dir_guid(UserRootDirUuid),
                    {NewCtx, #file_info{path = filename:join(Tokens), guid = UserRootDirGuid}};
                [<<"/">>, SpaceName | Rest] ->
                    {#document{value = #od_user{space_aliases = Spaces}}, Ctx2} =
                        fslogic_context:get_user(Ctx),

                    case lists:keyfind(SpaceName, 2, Spaces) of
                        false ->
                            throw(?ENOENT);
                        {SpaceId, _} ->
                            {Ctx2, #file_info{path = filename:join([<<"/">>, SpaceId | Rest])}}
                    end
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create new file_info using file's guid
%% @end
%%--------------------------------------------------------------------
-spec new_by_guid(guid()) -> file_info().
new_by_guid(Guid) ->
    #file_info{guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Create new file_info using file's guid
%% @end
%%--------------------------------------------------------------------
-spec new_by_doc(file_meta:doc(), od_space:id()) -> file_info().
new_by_doc(Doc = #document{key = Uuid, value = #file_meta{}}, SpaceId) ->
    Guid = fslogic_uuid:uuid_to_guid(Uuid, SpaceId),
    #file_info{file_doc = Doc, guid = Guid}.

%%--------------------------------------------------------------------
%% @doc
%% Get file's share_id.
%% @end
%%--------------------------------------------------------------------
-spec get_share_id(fslogic_context:ctx()) -> od_share:id() | undefined.
get_share_id(Ctx) -> %todo TL return share_id cached in file_info
    fslogic_context:get_share_id(Ctx).

%%--------------------------------------------------------------------
%% @doc
%% Get file's cannonical path (starting with "/SpaceId/....
%% @end
%%--------------------------------------------------------------------
-spec get_path(file_info()) -> path().
get_path(#file_info{path = Path}) ->
    Path.

%%--------------------------------------------------------------------
%% @doc
%% Get file's SpaceId
%% @end
%%--------------------------------------------------------------------
-spec get_space_id(file_info()) -> od_space:id().
get_space_id(#file_info{space_dir_doc = #document{key = SpaceUuid}}) ->
    fslogic_uuid:space_dir_uuid_to_spaceid(SpaceUuid);
get_space_id(#file_info{guid = undefined, path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, SpaceId | _] ->
            SpaceId;
        _ ->
            undefined
    end;
get_space_id(#file_info{guid = Guid}) ->
    fslogic_uuid:guid_to_space_id(Guid).

%%--------------------------------------------------------------------
%% @doc
%% Get file's SpaceDir uuid
%% @end
%%--------------------------------------------------------------------
-spec get_space_dir_uuid(file_info()) -> od_space:id().
get_space_dir_uuid(#file_info{space_dir_doc = #document{key = SpaceUuid}}) ->
    SpaceUuid;
get_space_dir_uuid(FileInfo) ->
    fslogic_uuid:spaceid_to_space_dir_uuid(get_space_id(FileInfo)).

%%--------------------------------------------------------------------
%% @doc
%% Get file's guid
%% @end
%%--------------------------------------------------------------------
-spec get_guid(file_info()) -> {fslogic_worker:file_guid(), file_info()}.
get_guid(FileInfo = #file_info{guid = undefined, path = Path}) ->
    {ok, Uuid} = file_meta:to_uuid({path, Path}),
    SpaceId = get_space_id(FileInfo),
    Guid = fslogic_uuid:uuid_to_guid(Uuid, SpaceId),
    {Guid, FileInfo#file_info{guid = Guid}};
get_guid(FileInfo = #file_info{guid = Guid}) ->
    {Guid, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get file's file_meta document.
%% @end
%%--------------------------------------------------------------------
-spec get_file_doc(file_info()) -> {file_meta:doc() | {error, term()}, file_info()}.
get_file_doc(FileInfo = #file_info{file_doc = undefined}) ->
    {Guid, NewFileInfo} = get_guid(FileInfo),
    case file_meta:get({uuid, fslogic_uuid:guid_to_uuid(Guid)}) of
        {ok, FileDoc} ->
            {FileDoc, NewFileInfo#file_info{file_doc = FileDoc}};
        Error ->
            {Error, NewFileInfo#file_info{file_doc = Error}}
    end;
get_file_doc(FileInfo = #file_info{file_doc = FileDoc}) ->
    {FileDoc, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get parent's file_info.
%% @end
%%--------------------------------------------------------------------
-spec get_parent(file_info()) -> {ParentFileInfo :: file_info(), NewFileInfo :: file_info()}.
get_parent(FileInfo = #file_info{parent = undefined}) ->
    {Doc, NewFileInfo} = file_info:get_file_doc(FileInfo),
    SpaceId = file_info:get_space_id(NewFileInfo),
    {ok, ParentDoc} = file_meta:get_parent(Doc),

    Parent = file_info:new_by_doc(ParentDoc, SpaceId),
    {Parent, NewFileInfo#file_info{parent = Parent}};
get_parent(FileInfo = #file_info{parent = Parent}) ->
    {Parent, FileInfo}.

%%--------------------------------------------------------------------
%% @doc
%% Get storage file id (the id of file on storage. In case of posix it is its path on storage)
%% @end
%%--------------------------------------------------------------------
-spec get_storage_file_id(file_info()) -> {StorageFileId :: helpers:file(), file_info()}.
get_storage_file_id(FileInfo) ->
    {Guid, NewFileInfo} = get_guid(FileInfo),
    FileId = fslogic_utils:gen_storage_file_id({uuid, fslogic_uuid:guid_to_uuid(Guid)}), %todo TL do not use this util function, as it it overcomplicated
    {FileId, NewFileInfo#file_info{storage_file_id = FileId}}.

%%--------------------------------------------------------------------
%% @doc
%% Check if given argument contains file_info record
%% @end
%%--------------------------------------------------------------------
-spec is_file_info(file_info()) -> boolean().
is_file_info(#file_info{}) ->
    true;
is_file_info(_) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is a space root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_space_dir(file_info()) -> boolean().
is_space_dir(#file_info{guid = Guid}) when is_binary(Guid) ->
    SpaceId = (catch fslogic_uuid:space_dir_uuid_to_spaceid(fslogic_uuid:guid_to_uuid(Guid))),
    is_binary(SpaceId);
is_space_dir(#file_info{path = Path}) ->
    case fslogic_path:split(Path) of
        [<<"/">>, _SpaceId] ->
            true;
        _ ->
            false
    end.

%%--------------------------------------------------------------------
%% @doc
%% Check if file is an user root dir.
%% @end
%%--------------------------------------------------------------------
-spec is_user_root_dir(file_info(), fslogic_context:ctx()) -> {boolean(), NewCtx :: fslogic_context:ctx()}.
is_user_root_dir(#file_info{guid = undefined, path = <<"/">>}, Ctx) ->
    {true, Ctx};
is_user_root_dir(#file_info{guid = undefined}, Ctx) ->
    {false, Ctx};
is_user_root_dir(#file_info{guid = Guid}, Ctx) ->
    {UserRootDirUuid, NewCtx} = fslogic_context:get_user_root_dir_uuid(Ctx),
    {UserRootDirUuid == fslogic_uuid:guid_to_uuid(Guid), NewCtx}.

%%%===================================================================
%%% Internal functions
%%%===================================================================