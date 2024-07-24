%%%-------------------------------------------------------------------
%%% @author Michal Stanisz
%%% @copyright (C) 2024 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Module responsible for consistent handling of special dirs' documents.
%%% @end
%%%-------------------------------------------------------------------
-module(special_dir_docs).
-author("Michal Stanisz").


-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_runner.hrl").


% API
-export([create/3, add_parent_link/4, delete_parent_link/4]).

%%%===================================================================
%%% API
%%%===================================================================

-spec create(od_space:id(), file_meta:doc(), add_link | no_link) -> created | exists.
create(SpaceId, #document{key = Uuid, value = #file_meta{name = Name, parent_uuid = ParentUuid} = FM} = Doc, LinkPolicy) ->
    FinalDoc = Doc#document{value = FM#file_meta{provider_id = oneprovider:get_id()}},
    Result = case file_meta:update(Uuid, fun(_) -> {error, already_exists} end, FinalDoc) of
        {ok, CreatedDoc} ->
            times_api:report_file_created(file_ctx:new_by_doc(CreatedDoc, SpaceId)),
            created;
        {error, already_exists} ->
            exists
    end,
    case LinkPolicy of
        add_link -> add_parent_link(ParentUuid, SpaceId, Name, Uuid);
        no_link -> ok
    end,
    Result.


-spec add_parent_link(file_meta:uuid(), od_space:id(), file_meta:name(), file_meta:uuid()) -> ok | {error, term()}.
add_parent_link(ParentUuid, SpaceId, Name, Uuid) ->
    ok = ?ok_if_exists(?extract_ok(file_meta_forest:add(ParentUuid, SpaceId, Name, Uuid))).


-spec delete_parent_link(file_meta:uuid(), od_space:id(), file_meta:name(), file_meta:uuid()) -> ok | {error, term()}.
delete_parent_link(ParentUuid, SpaceId, Name, Uuid) ->
    ok = ?ok_if_not_found(file_meta_forest:delete(ParentUuid, SpaceId, Name, Uuid)).
