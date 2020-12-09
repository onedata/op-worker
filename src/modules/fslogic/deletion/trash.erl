%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% API module for trash management.
%%% WRITEME napisac, ze kosz ma konkretną nazwę i uuid
%%% WRITEME napisac, ze kosz ma miec docelowo zduplikowane linki i ze beda filtrowane w momencie rozwiazywania konfliktow przy ls
%%% @end
%%%-------------------------------------------------------------------
-module(trash).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/errors.hrl").


%% API
-export([create/1, move_to_trash/1, delete_from_trash/3]).

% todo po usunieciu pamietac o usunieciu linka z kosza, moze w task_finished?
% todo dodac test do usuwania w trakcie importu
%% todo zabezpieczony przed usuwaniem, replikacją (ale ewikcja możliwa)
%% delete przez REST/GUI zawsze robi move do kosza i zleca traverse na usunięcie

%% todo zabezpieczyc przed usuwaniem atalogi protected
%% todo zabezpieczyc przed mv atalogi protected
%% todo zabezpieczyc kosz przed replikacja !!!

%% zlecania usuwania z sesją usera, ktory przeniósł do kosza
%% usuwanie całego kosza - poszczegolne elementy z sesjami userów, którzy przeniesli do kosza
%% jak poradzic sobie z gasnącą sesją?

-define(NAME_UUID_SEPARATOR, "@@").
-define(NAME_IN_TRASH(FileName, FileUuid), <<FileName/binary, ?NAME_UUID_SEPARATOR, FileUuid/binary>>).

% todo a co jak ktos zleci usunięcie katalogu spejsa? powinnismy wyczyscic wszystko
% ale czy przenosimy katalog spejsa do kosza?

%%%===================================================================
%%% API functions
%%%===================================================================

-spec create(od_space:id()) -> ok.
create(SpaceId) ->
    SpaceUuid = fslogic_uuid:spaceid_to_space_dir_uuid(SpaceId),
    TrashDoc = file_meta:new_doc(fslogic_uuid:spaceid_to_trash_dir_uuid(SpaceId),
        ?TRASH_DIR_NAME, ?DIRECTORY_TYPE, ?TRASH_DIR_PERMS, ?SPACE_OWNER_ID(SpaceId), SpaceUuid, SpaceId),
    % TODO VFS-7064 use file_meta:create so that link to the trash directory will be added
    %% {ok, _} = file_meta:create({uuid, SpaceUuid}, TrashDoc),
    {ok, _} = file_meta:save(TrashDoc),
    ok.


-spec move_to_trash(file_ctx:ctx()) -> ok.
move_to_trash(FileCtx) ->
    file_ctx:assert_not_protected_const(FileCtx),
    move_file_to_trash_internal(FileCtx).


-spec delete_from_trash(file_ctx:ctx(), user_ctx:ctx(), boolean()) -> {ok, tree_deletion_traverse:id()}.
delete_from_trash(FileCtx, UserCtx, Silent) ->
    file_ctx:assert_not_protected_const(FileCtx),
    tree_deletion_traverse:start(FileCtx, UserCtx, Silent).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec move_file_to_trash_internal(file_ctx:ctx()) -> ok.
move_file_to_trash_internal(FileCtx) ->
    SpaceId = file_ctx:get_space_id_const(FileCtx),
    Uuid = file_ctx:get_uuid_const(FileCtx),
    UserCtx = user_ctx:new(?ROOT_SESS_ID),
    {Name, FileCtx2} = file_ctx:get_aliased_name(FileCtx, UserCtx),
    {ParentGuid, _} = file_ctx:get_parent_guid(FileCtx2, UserCtx),
    {ParentUuid, _} = file_id:unpack_guid(ParentGuid),
    TrashUuid = fslogic_uuid:spaceid_to_trash_dir_uuid(SpaceId),
    % files moved to trash are children of trash directory
    % they names are suffixed with Uuid to avoid conflicts
    file_meta_links:add(TrashUuid, SpaceId, ?NAME_IN_TRASH(Name, Uuid), Uuid),
    % uwaga z tym deletion markerem zeby sie zgadzala nazwa, bo teraz dodamy linka o innej nazwie troche !!!
    % add deletion_marker to ensure that file won't be reimported by import % todo alwawys czy tylko na imported?
    deletion_marker:add(ParentUuid, FileCtx),
    file_meta:delete_child_link(ParentUuid, SpaceId, Name, Uuid).
    % todo zlecic usuwanie traversem ,tu albo gdzies indziej?