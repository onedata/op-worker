%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions used in API tests.
%%% @end
%%%-------------------------------------------------------------------
-module(api_test_utils).
-author("Bartosz Walkowicz").

-include("api_test_runner.hrl").
-include("api_file_test_utils.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("test_utils/initializer.hrl").


-export([
    build_rest_url/2,

    create_shared_file_in_space_krk/0,
    create_and_sync_shared_file_in_space_krk_par/1,
    create_and_sync_shared_file_in_space_krk_par/2,
    create_and_sync_shared_file_in_space_krk_par/3,
    create_file_in_space_krk_par_with_additional_metadata/3,
    create_file_in_space_krk_par_with_additional_metadata/4,

    randomly_choose_file_type_for_test/0,
    randomly_choose_file_type_for_test/1,

    share_file_and_sync_file_attrs/4,

    set_and_sync_metadata/4,
    set_metadata/4,
    get_metadata/3,
    set_xattrs/3,
    get_xattrs/2,

    randomly_add_qos/4,
    randomly_set_metadata/2,
    randomly_set_acl/2,
    randomly_create_share/3,

    guids_to_object_ids/1,
    file_details_to_gs_json/2,
    file_attrs_to_json/2
]).
-export([
    add_file_id_errors_for_operations_available_in_share_mode/3,
    add_file_id_errors_for_operations_available_in_share_mode/4,

    add_file_id_errors_for_operations_not_available_in_share_mode/3,
    add_file_id_errors_for_operations_not_available_in_share_mode/4,

    add_cdmi_id_errors_for_operations_not_available_in_share_mode/4,
    add_cdmi_id_errors_for_operations_not_available_in_share_mode/5,

    replace_enoent_with_error_not_found_in_error_expectations/1,
    maybe_substitute_bad_id/2
]).

-type file_type() :: binary(). % <<"file">> | <<"dir">>
-type metadata_type() :: binary().  % <<"rdf">> | <<"json">> | <<"xattrs">>.

-export_type([file_type/0, metadata_type/0]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec build_rest_url(node(), [binary()]) -> binary().
build_rest_url(Node, PathTokens) ->
    rpc:call(Node, oneprovider, build_rest_url, [PathTokens]).


-spec create_shared_file_in_space_krk() ->
    {file_type(), file_meta:path(), file_id:file_guid(), od_share:id()}.
create_shared_file_in_space_krk() ->
    [P1Node] = oct_background:get_provider_nodes(krakow),

    UserSessId = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessId = oct_background:get_user_session_id(user1, krakow),

    FileType = randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_KRK, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = lfm_test_utils:create_file(FileType, P1Node, UserSessId, FilePath),
    {ok, ShareId} = opt_shares:create(P1Node, SpaceOwnerSessId, ?FILE_REF(FileGuid), <<"share">>),

    {FileType, FilePath, FileGuid, ShareId}.


-spec create_and_sync_shared_file_in_space_krk_par(file_meta:mode()) ->
    {file_type(), file_meta:path(), file_id:file_guid(), od_share:id()}.
create_and_sync_shared_file_in_space_krk_par(Mode) ->
    FileType = randomly_choose_file_type_for_test(),
    create_and_sync_shared_file_in_space_krk_par(FileType, Mode).


-spec create_and_sync_shared_file_in_space_krk_par(file_type(), file_meta:mode()) ->
    {file_type(), file_meta:path(), file_id:file_guid(), od_share:id()}.
create_and_sync_shared_file_in_space_krk_par(FileType, Mode) ->
    create_and_sync_shared_file_in_space_krk_par(FileType, ?RANDOM_FILE_NAME(), Mode).


-spec create_and_sync_shared_file_in_space_krk_par(
    file_type(),
    file_meta:name(),
    file_meta:mode()
) ->
    {file_type(), file_meta:path(), file_id:file_guid(), od_share:id()}.
create_and_sync_shared_file_in_space_krk_par(FileType, FileName, Mode) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    SpaceOwnerSessIdP1 = kv_utils:get([users, user2, sessions, krakow], node_cache:get(oct_mapping)),
    UserSessIdP1 = kv_utils:get([users, user3, sessions, krakow], node_cache:get(oct_mapping)),

    FilePath = filename:join(["/", ?SPACE_KRK_PAR, FileName]),
    {ok, FileGuid} = lfm_test_utils:create_file(FileType, P1Node, UserSessIdP1, FilePath, Mode),
    {ok, ShareId} = opt_shares:create(P1Node, SpaceOwnerSessIdP1, ?FILE_REF(FileGuid), <<"share">>),

    file_test_utils:await_sync(P2Node, FileGuid),

    {FileType, FilePath, FileGuid, ShareId}.


-spec create_file_in_space_krk_par_with_additional_metadata(
    file_meta:path(),
    boolean(),
    file_meta:name()
) ->
    {file_type(), file_meta:path(), file_id:file_guid(), #file_details{}}.
create_file_in_space_krk_par_with_additional_metadata(ParentPath, HasParentQos, FileName) ->
    FileType = randomly_choose_file_type_for_test(false),
    create_file_in_space_krk_par_with_additional_metadata(ParentPath, HasParentQos, FileType, FileName).


-spec create_file_in_space_krk_par_with_additional_metadata(
    file_meta:path(),
    boolean(),
    file_type(),
    file_meta:name()
) ->
    {file_type(), file_meta:path(), file_id:file_guid(), #file_details{}}.
create_file_in_space_krk_par_with_additional_metadata(ParentPath, HasParentQos, FileType, FileName) ->
    [P1Node] = oct_background:get_provider_nodes(krakow),
    [P2Node] = oct_background:get_provider_nodes(paris),
    Nodes = [P1Node, P2Node],

    UserSessIdP1 = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessIdP1 = oct_background:get_user_session_id(user2, krakow),

    FilePath = filename:join([ParentPath, FileName]),

    FileMode = lists_utils:random_element([8#707, 8#705, 8#700]),
    {ok, FileGuid} = lfm_test_utils:create_file(
        FileType, P1Node, UserSessIdP1, FilePath, FileMode
    ),
    FileShares = case randomly_create_share(P1Node, SpaceOwnerSessIdP1, FileGuid) of
        undefined -> [];
        ShareId -> [ShareId]
    end,
    Size = case FileType of
        <<"file">> ->
            RandSize = rand:uniform(20),
            lfm_test_utils:write_file(P1Node, SpaceOwnerSessIdP1, FileGuid, {rand_content, RandSize}),
            RandSize;
        <<"dir">> ->
            undefined
    end,
    {ok, FileAttrs} = ?assertMatch(
        {ok, #file_attr{size = Size, shares = FileShares}},
        file_test_utils:get_attrs(P2Node, FileGuid),
        ?ATTEMPTS
    ),

    HasDirectQos = randomly_add_qos(Nodes, FileGuid, <<"key=value2">>, 2),
    HasMetadata = randomly_set_metadata(Nodes, FileGuid),
    HasAcl = randomly_set_acl(Nodes, FileGuid),

    FileDetails = #file_details{
        file_attr = FileAttrs,
        active_permissions_type = case HasAcl of
            true -> acl;
            false -> posix
        end,
        eff_protection_flags = ?no_flags_mask,
        eff_qos_membership = case {HasDirectQos, HasParentQos} of
            {true, true} -> ?DIRECT_AND_ANCESTOR_MEMBERSHIP;
            {true, _} -> ?DIRECT_MEMBERSHIP;
            {_, true} -> ?ANCESTOR_MEMBERSHIP;
            _ -> ?NONE_MEMBERSHIP
        end,
        eff_dataset_membership = ?NONE_MEMBERSHIP,
        has_metadata = HasMetadata
    },

    {FileType, FilePath, FileGuid, FileDetails}.


-spec randomly_choose_file_type_for_test() -> file_type().
randomly_choose_file_type_for_test() ->
    randomly_choose_file_type_for_test(true).


-spec randomly_choose_file_type_for_test(boolean()) -> file_type().
randomly_choose_file_type_for_test(LogSelectedFileType) ->
    FileType = ?RANDOM_FILE_TYPE(),
    LogSelectedFileType andalso ct:pal("Chosen file type for test: ~s", [FileType]),
    FileType.


-spec share_file_and_sync_file_attrs(node(), session:id(), [node()], file_id:file_guid()) ->
    od_share:id().
share_file_and_sync_file_attrs(CreationNode, SessionId, SyncNodes, FileGuid) ->
    {ok, ShareId} = ?assertMatch(
        {ok, _},
        opt_shares:create(CreationNode, SessionId, ?FILE_REF(FileGuid), <<"share">>),
        ?ATTEMPTS
    ),
    lists:foreach(fun(Node) ->
        ?assertMatch(
            {ok, #file_attr{shares = [ShareId | _]}},
            file_test_utils:get_attrs(Node, FileGuid),
            ?ATTEMPTS
        )
    end, SyncNodes),

    ShareId.


-spec set_and_sync_metadata([node()], file_id:file_guid(), metadata_type(), term()) -> ok.
set_and_sync_metadata(Nodes, FileGuid, MetadataType, Metadata) ->
    RandNode = lists_utils:random_element(Nodes),
    ?assertMatch(ok, set_metadata(RandNode, FileGuid, MetadataType, Metadata), ?ATTEMPTS),

    lists:foreach(fun(Node) ->
        ?assertMatch({ok, Metadata}, get_metadata(Node, FileGuid, MetadataType), ?ATTEMPTS)
    end, Nodes).


-spec set_metadata(node(), file_id:file_guid(), metadata_type(), term()) -> ok.
set_metadata(Node, FileGuid, <<"rdf">>, Metadata) ->
    opt_file_metadata:set_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), rdf, Metadata, []);
set_metadata(Node, FileGuid, <<"json">>, Metadata) ->
    opt_file_metadata:set_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), json, Metadata, []);
set_metadata(Node, FileGuid, <<"xattrs">>, Metadata) ->
    set_xattrs(Node, FileGuid, Metadata).


-spec get_metadata(node(), file_id:file_guid(), metadata_type()) -> {ok, term()}.
get_metadata(Node, FileGuid, <<"rdf">>) ->
    opt_file_metadata:get_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), rdf, [], false);
get_metadata(Node, FileGuid, <<"json">>) ->
    opt_file_metadata:get_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), json, [], false);
get_metadata(Node, FileGuid, <<"xattrs">>) ->
    get_xattrs(Node, FileGuid).


-spec set_xattrs(node(), file_id:file_guid(), map()) -> ok.
set_xattrs(Node, FileGuid, Xattrs) ->
    lists:foreach(fun({Key, Val}) ->
        ?assertMatch(ok, lfm_proxy:set_xattr(
            Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), #xattr{
                name = Key,
                value = Val
            }
        ), ?ATTEMPTS)
    end, maps:to_list(Xattrs)).


-spec get_xattrs(node(), file_id:file_guid()) -> {ok, map()}.
get_xattrs(Node, FileGuid) ->
    FileKey = ?FILE_REF(FileGuid),

    {ok, Keys} = ?assertMatch(
        {ok, _}, lfm_proxy:list_xattr(Node, ?ROOT_SESS_ID, FileKey, false, true), ?ATTEMPTS
    ),
    {ok, lists:foldl(fun(Key, Acc) ->
        % Check in case of race between listing xattrs and fetching xattr value
        case lfm_proxy:get_xattr(Node, ?ROOT_SESS_ID, FileKey, Key) of
            {ok, #xattr{name = Name, value = Value}} ->
                Acc#{Name => Value};
            {error, _} ->
                Acc
        end
    end, #{}, Keys)}.


-spec randomly_add_qos([node()], file_id:file_guid(), qos_expression:expression(), qos_entry:replicas_num()) ->
    Added :: boolean().
randomly_add_qos(Nodes, FileGuid, Expression, ReplicasNum) ->
    case rand:uniform(2) of
        1 ->
            RandNode = lists_utils:random_element(Nodes),
            {ok, QosEntryId} = ?assertMatch({ok, _}, opt_qos:add_qos_entry(
                RandNode, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), Expression, ReplicasNum
            ), ?ATTEMPTS),
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, _}, opt_qos:get_qos_entry(Node, ?ROOT_SESS_ID, QosEntryId), ?ATTEMPTS)
            end, Nodes),
            true;
        2 ->
            false
    end.


-spec randomly_set_metadata([node()], file_id:file_guid()) -> Set :: boolean().
randomly_set_metadata(Nodes, FileGuid) ->
    case rand:uniform(2) of
        1 ->
            FileKey = ?FILE_REF(FileGuid),
            RandNode = lists_utils:random_element(Nodes),
            ?assertMatch(ok, opt_file_metadata:set_custom_metadata(
                RandNode, ?ROOT_SESS_ID, FileKey, rdf, ?RDF_METADATA_1, []
            ), ?ATTEMPTS),
            lists:foreach(fun(Node) ->
                ?assertMatch(
                    {ok, _},
                    opt_file_metadata:get_custom_metadata(Node, ?ROOT_SESS_ID, FileKey, rdf, [], false),
                    ?ATTEMPTS
                )
            end, Nodes),
            true;
        2 ->
            false
    end.


-spec randomly_set_acl([node()], file_id:file_guid()) -> Set ::boolean().
randomly_set_acl(Nodes, FileGuid) ->
    case rand:uniform(2) of
        1 ->
            FileKey = ?FILE_REF(FileGuid),
            RandNode = lists_utils:random_element(Nodes),
            ?assertMatch(ok, lfm_proxy:set_acl(
                RandNode, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), acl:from_json(?OWNER_ONLY_ALLOW_ACL, cdmi)
            ), ?ATTEMPTS),
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, [_]}, lfm_proxy:get_acl(Node, ?ROOT_SESS_ID, FileKey), ?ATTEMPTS)
            end, Nodes),
            true;
        2 ->
            false
    end.


-spec randomly_create_share(node(), session:id(), file_id:file_guid()) ->
    ShareId :: undefined | od_share:id().
randomly_create_share(Node, SessionId, FileGuid) ->
    case rand:uniform(2) of
        1 ->
            {ok, ShId} = ?assertMatch({ok, _}, opt_shares:create(
                Node, SessionId, ?FILE_REF(FileGuid), <<"share">>
            )),
            ShId;
        2 ->
            undefined
    end.


-spec guids_to_object_ids([file_id:file_guid()]) -> [file_id:objectid()].
guids_to_object_ids(Guids) ->
    lists:map(fun(Guid) ->
        {ok, ObjectId} = file_id:guid_to_objectid(Guid),
        ObjectId
    end, Guids).


-spec file_details_to_gs_json(undefined | od_share:id(), #file_details{}) -> map().
file_details_to_gs_json(undefined, #file_details{
    file_attr = #file_attr{
        guid = FileGuid,
        parent_guid = ParentGuid,
        name = FileName,
        type = Type,
        mode = Mode,
        size = Size,
        mtime = MTime,
        shares = Shares,
        owner_id = OwnerId,
        provider_id = ProviderId,
        nlink = LinksCount,
        listing_index = Index
    },
    active_permissions_type = ActivePermissionsType,
    eff_protection_flags = EffFileProtectionFlags,
    eff_qos_membership = EffQosMembership,
    eff_dataset_membership = EffDatasetMembership,
    has_metadata = HasMetadata,
    recall_root_id = RecallRootId
}) ->
    DisplayedSize = case Type of
        ?DIRECTORY_TYPE -> null;
        _ -> Size
    end,

    #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => FileGuid,
        <<"name">> => FileName,
        <<"index">> => file_listing:encode_index(Index),
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
        <<"effProtectionFlags">> => file_meta:protection_flags_to_json(EffFileProtectionFlags),
        % For space dir gs returns null as parentId instead of user root dir
        % (gui doesn't know about user root dir)
        <<"parentId">> => case fslogic_file_id:is_space_dir_guid(FileGuid) of
            true -> null;
            false -> ParentGuid
        end,
        <<"mtime">> => MTime,
        <<"type">> => str_utils:to_binary(Type),
        <<"size">> => DisplayedSize,
        <<"shares">> => Shares,
        <<"activePermissionsType">> => atom_to_binary(ActivePermissionsType, utf8),
        <<"providerId">> => ProviderId,
        <<"ownerId">> => OwnerId,
        <<"effQosMembership">> => translate_membership(EffQosMembership),
        <<"effDatasetMembership">> => translate_membership(EffDatasetMembership),
        <<"hardlinksCount">> => utils:undefined_to_null(LinksCount),
        <<"recallRootId">> => utils:undefined_to_null(RecallRootId)
    };
file_details_to_gs_json(ShareId, #file_details{
    file_attr = #file_attr{
        guid = FileGuid,
        parent_guid = ParentGuid,
        name = FileName,
        type = Type,
        mode = Mode,
        size = Size,
        mtime = MTime,
        shares = Shares,
        listing_index = Index
    },
    active_permissions_type = ActivePermissionsType,
    has_metadata = HasMetadata
}) ->
    DisplayedSize = case Type of
        ?DIRECTORY_TYPE -> null;
        _ -> Size
    end,
    IsShareRoot = lists:member(ShareId, Shares),

    #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => file_id:guid_to_share_guid(FileGuid, ShareId),
        <<"name">> => FileName,
        <<"index">> => file_listing:encode_index(Index),
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode band 2#111, 8), 3, $0)),
        <<"parentId">> => case IsShareRoot of
            true -> null;
            false -> file_id:guid_to_share_guid(ParentGuid, ShareId)
        end,
        <<"mtime">> => MTime,
        <<"type">> => str_utils:to_binary(Type),
        <<"size">> => DisplayedSize,
        <<"shares">> => case IsShareRoot of
            true -> [ShareId];
            false -> []
        end,
        <<"activePermissionsType">> => atom_to_binary(ActivePermissionsType, utf8)
    }.


-spec file_attrs_to_json(undefined | od_share:id(), #file_attr{}) -> map().
file_attrs_to_json(undefined, #file_attr{
    guid = Guid,
    name = Name,
    mode = Mode,
    parent_guid = ParentGuid,
    uid = Uid,
    gid = Gid,
    atime = Atime,
    mtime = Mtime,
    ctime = Ctime,
    type = Type,
    size = Size,
    shares = Shares,
    provider_id = ProviderId,
    owner_id = OwnerId,
    nlink = HardlinksCount,
    listing_index = ListingIndex
}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(Guid),
    
    #{
        <<"file_id">> => ObjectId,
        <<"name">> => Name,
        <<"mode">> => list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
        <<"parent_id">> => case ParentGuid of
            undefined ->
                null;
            _ ->
                {ok, ParentObjectId} = file_id:guid_to_objectid(ParentGuid),
                ParentObjectId
        end,
        <<"storage_user_id">> => Uid,
        <<"storage_group_id">> => Gid,
        <<"atime">> => Atime,
        <<"mtime">> => Mtime,
        <<"ctime">> => Ctime,
        <<"type">> => str_utils:to_binary(Type),
        <<"size">> => case Type of
            ?DIRECTORY_TYPE -> null;
            _ -> utils:undefined_to_null(Size)
        end,
        <<"shares">> => Shares,
        <<"provider_id">> => ProviderId,
        <<"owner_id">> => OwnerId,
        <<"hardlinks_count">> => utils:undefined_to_null(HardlinksCount),
        <<"listing_index">> => ListingIndex
    };
file_attrs_to_json(ShareId, #file_attr{
    guid = FileGuid,
    parent_guid = ParentGuid,
    name = Name,
    type = Type,
    mode = Mode,
    size = Size,
    mtime = Mtime,
    atime = Atime,
    ctime = Ctime,
    shares = Shares
}) ->
    {ok, ObjectId} = file_id:guid_to_objectid(file_id:guid_to_share_guid(FileGuid, ShareId)),
    IsShareRoot = lists:member(ShareId, Shares),
    
    #{
        <<"file_id">> => ObjectId,
        <<"name">> => Name,
        <<"mode">> => list_to_binary(string:right(integer_to_list(Mode band 2#111, 8), 3, $0)),
        <<"parent_id">> => case IsShareRoot of
            true -> null;
            false ->
                {ok, ParentObjectId} = file_id:guid_to_objectid(file_id:guid_to_share_guid(ParentGuid, ShareId)),
                ParentObjectId
        end,
        <<"atime">> => Atime,
        <<"mtime">> => Mtime,
        <<"ctime">> => Ctime,
        <<"type">> => str_utils:to_binary(Type),
        <<"size">> => case Type of
            ?DIRECTORY_TYPE -> null;
            _ -> utils:undefined_to_null(Size)
        end,
        <<"shares">> => case IsShareRoot of
            true -> [ShareId];
            false -> []
        end
    }.

%%--------------------------------------------------------------------
%% @doc
%% Adds to data_spec() errors for invalid file id's (guid, path, cdmi_id) for
%% either normal and share mode (since operation is available in both modes
%% it is expected that it will have distinct tests for each mode).
%%
%% ATTENTION !!!
%%
%% Bad ids are available under 'bad_id' atom key - test implementation should
%% make sure to substitute them for fileId component in rest path or #gri.id
%% before making test call.
%% @end
%%--------------------------------------------------------------------
-spec add_file_id_errors_for_operations_available_in_share_mode(
    file_id:file_guid(),
    undefined | od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_available_in_share_mode(FileGuid, ShareId, DataSpec) ->
    add_file_id_errors_for_operations_available_in_share_mode(<<"id">>, FileGuid, ShareId, DataSpec).


-spec add_file_id_errors_for_operations_available_in_share_mode(
    IdKey :: binary(),
    file_id:file_guid(),
    undefined | od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_available_in_share_mode(IdKey, FileGuid, ShareId, DataSpec) ->
    InvalidFileIdErrors = get_invalid_file_id_errors(IdKey),
    NonExistentSpaceGuid = file_id:pack_share_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID, ShareId),
    SpaceId = file_id:guid_to_space_id(FileGuid),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),
    NonExistentSpaceExpError = case ShareId of
        undefined ->
            % For authenticated users it should fail on authorization step
            % (checks if user belongs to space)
            ?ERROR_FORBIDDEN;
        _ ->
            % For share request it should fail on validation step
            % (checks if space is supported by provider)
            {error_fun, fun(#api_test_ctx{node = Node}) ->
                ProvId = opw_test_rpc:get_provider_id(Node),
                ?ERROR_SPACE_NOT_SUPPORTED_BY(?NOT_SUPPORTED_SPACE_ID, ProvId)
            end}
    end,

    NonExistentFileGuid = file_id:pack_share_guid(<<"InvalidUuid">>, SpaceId, ShareId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    BadFileIdErrors = InvalidFileIdErrors ++ [
        {bad_id, NonExistentSpaceObjectId, {rest, NonExistentSpaceExpError}},
        {bad_id, NonExistentSpaceGuid, {gs, NonExistentSpaceExpError}},

        % Errors thrown by internal logic (all middleware checks were passed)
        {bad_id, NonExistentFileObjectId, {rest, ?ERROR_POSIX(?ENOENT)}},
        {bad_id, NonExistentFileGuid, {gs, ?ERROR_POSIX(?ENOENT)}}
    ],

    add_bad_values_to_data_spec(BadFileIdErrors, DataSpec).


%%--------------------------------------------------------------------
%% @doc
%% Adds to data_spec() errors for invalid file id's (guid, path, cdmi_id)
%% for both normal and share mode (since operation is not available in share
%% mode there is no need to write distinct test for share mode - access errors
%% when using share file id can be checked along with other bad_values errors).
%%
%% ATTENTION !!!
%%
%% Bad ids are available under 'bad_id' atom key - test implementation should
%% make sure to substitute them for fileId component in rest path or #gri.id
%% before making test call.
%% @end
%%--------------------------------------------------------------------
-spec add_file_id_errors_for_operations_not_available_in_share_mode(
    file_id:file_guid(),
    od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_not_available_in_share_mode(FileGuid, ShareId, DataSpec) ->
    add_file_id_errors_for_operations_not_available_in_share_mode(<<"id">>, FileGuid, ShareId, DataSpec).


-spec add_file_id_errors_for_operations_not_available_in_share_mode(
    IdKey :: binary(),
    file_id:file_guid(),
    od_share:id(),
    undefined | onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_file_id_errors_for_operations_not_available_in_share_mode(IdKey, FileGuid, ShareId, DataSpec) ->
    InvalidFileIdErrors = get_invalid_file_id_errors(IdKey),

    NonExistentSpaceGuid = file_id:pack_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),

    NonExistentSpaceErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        NonExistentSpaceGuid, ShareId, [
            % Errors in normal mode - thrown by middleware auth checks
            % (checks whether authenticated user belongs to space)
            {bad_id, NonExistentSpaceObjectId, {rest, ?ERROR_FORBIDDEN}},
            {bad_id, NonExistentSpaceGuid, {gs, ?ERROR_FORBIDDEN}}
        ]
    ),

    SpaceId = file_id:guid_to_space_id(FileGuid),
    NonExistentFileGuid = file_id:pack_guid(<<"InvalidUuid">>, SpaceId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    NonExistentFileErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        NonExistentFileGuid, ShareId, [
            % Errors in normal mode - thrown by internal logic
            % (all middleware checks were passed)
            {bad_id, NonExistentFileObjectId, {rest, ?ERROR_POSIX(?ENOENT)}},
            {bad_id, NonExistentFileGuid, {gs, ?ERROR_POSIX(?ENOENT)}}
        ]
    ),

    ShareFileErrors = add_share_file_id_errors_for_operations_not_available_in_share_mode(
        FileGuid, ShareId, []
    ),

    BadFileIdErrors = lists:flatten([
        InvalidFileIdErrors,
        NonExistentSpaceErrors,
        NonExistentFileErrors,
        ShareFileErrors
    ]),

    add_bad_values_to_data_spec(BadFileIdErrors, DataSpec).


%%--------------------------------------------------------------------
%% @doc
%% Extends data_spec() with file id bad values and errors for operations
%% not available in share mode that provide file id as parameter in data spec map.
%% All added bad values are in cdmi form and are stored under <<"fileId">> key.
%% @end
%%--------------------------------------------------------------------
-spec add_cdmi_id_errors_for_operations_not_available_in_share_mode(
    file_id:file_guid(),
    od_space:id(),
    od_share:id(),
    onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_cdmi_id_errors_for_operations_not_available_in_share_mode(FileGuid, SpaceId, ShareId, DataSpec) ->
    add_cdmi_id_errors_for_operations_not_available_in_share_mode(<<"fileId">>, FileGuid, SpaceId, ShareId, DataSpec).


-spec add_cdmi_id_errors_for_operations_not_available_in_share_mode(
    IdKey :: binary(),
    file_id:file_guid(),
    od_space:id(),
    od_share:id(),
    onenv_api_test_runner:data_spec()
) ->
    onenv_api_test_runner:data_spec().
add_cdmi_id_errors_for_operations_not_available_in_share_mode(IdKey, FileGuid, SpaceId, ShareId, DataSpec) ->
    {ok, DummyObjectId} = file_id:guid_to_objectid(<<"DummyGuid">>),

    NonExistentSpaceGuid = file_id:pack_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID),
    {ok, NonExistentSpaceObjectId} = file_id:guid_to_objectid(NonExistentSpaceGuid),

    NonExistentSpaceShareGuid = file_id:guid_to_share_guid(NonExistentSpaceGuid, ShareId),
    {ok, NonExistentSpaceShareObjectId} = file_id:guid_to_objectid(NonExistentSpaceShareGuid),

    NonExistentFileGuid = file_id:pack_guid(<<"InvalidUuid">>, SpaceId),
    {ok, NonExistentFileObjectId} = file_id:guid_to_objectid(NonExistentFileGuid),

    NonExistentFileShareGuid = file_id:guid_to_share_guid(NonExistentFileGuid, ShareId),
    {ok, NonExistentFileShareObjectId} = file_id:guid_to_objectid(NonExistentFileShareGuid),

    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareFileObjectId} = file_id:guid_to_objectid(ShareFileGuid),
    BadFileIdValues = [
        {IdKey, <<"InvalidObjectId">>, ?ERROR_BAD_VALUE_IDENTIFIER(IdKey)},
        {IdKey, DummyObjectId, ?ERROR_BAD_VALUE_IDENTIFIER(IdKey)},

        % user has no privileges in non existent space and so he should receive ?ERROR_FORBIDDEN
        {IdKey, NonExistentSpaceObjectId, ?ERROR_FORBIDDEN},
        {IdKey, NonExistentSpaceShareObjectId, ?ERROR_FORBIDDEN},

        {IdKey, NonExistentFileObjectId, ?ERROR_POSIX(?ENOENT)},

        % operation is not available in share mode - it should result in ?EPERM
        {IdKey, ShareFileObjectId, ?ERROR_POSIX(?EPERM)},
        {IdKey, NonExistentFileShareObjectId, ?ERROR_POSIX(?EPERM)}
    ],

    add_bad_values_to_data_spec(BadFileIdValues, DataSpec).


-spec replace_enoent_with_error_not_found_in_error_expectations(onenv_api_test_runner:data_spec()) ->
    onenv_api_test_runner:data_spec().
replace_enoent_with_error_not_found_in_error_expectations(DataSpec = #data_spec{bad_values = BadValues}) ->
    DataSpec#data_spec{bad_values = lists:map(fun
        ({Key, Value, ?ERROR_POSIX(?ENOENT)}) -> {Key, Value, ?ERROR_NOT_FOUND};
        ({Key, Value, {Interface, ?ERROR_POSIX(?ENOENT)}}) -> {Key, Value, {Interface, ?ERROR_NOT_FOUND}};
        (Spec) -> Spec
    end, BadValues)}.


maybe_substitute_bad_id(ValidId, undefined) ->
    {ValidId, undefined};
maybe_substitute_bad_id(ValidId, Data) ->
    case maps:take(bad_id, Data) of
        {BadId, LeftoverData} -> {BadId, LeftoverData};
        error -> {ValidId, Data}
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
add_bad_values_to_data_spec(BadValuesToAdd, undefined) ->
    #data_spec{bad_values = BadValuesToAdd};
add_bad_values_to_data_spec(BadValuesToAdd, #data_spec{bad_values = BadValues} = DataSpec) ->
    DataSpec#data_spec{bad_values = BadValuesToAdd ++ BadValues}.


%% @private
get_invalid_file_id_errors(IdKey) ->
    InvalidGuid = <<"InvalidGuid">>,
    {ok, InvalidObjectId} = file_id:guid_to_objectid(InvalidGuid),

    [
        % Errors thrown by rest_handler, which failed to convert file path/cdmi_id to guid
        {bad_id, <<"/NonExistentPath">>, {rest_with_file_path, ?ERROR_POSIX(?ENOENT)}},
        {bad_id, <<"InvalidObjectId">>, {rest, ?ERROR_SPACE_NOT_SUPPORTED_BY(<<"InvalidObjectId">>, provider_id_placeholder)}},

        % Errors thrown by middleware and internal logic
        {bad_id, InvalidObjectId, {rest, ?ERROR_SPACE_NOT_SUPPORTED_BY(InvalidObjectId, provider_id_placeholder)}},
        {bad_id, InvalidGuid, {gs, ?ERROR_BAD_VALUE_IDENTIFIER(IdKey)}}
    ].


%% @private
add_share_file_id_errors_for_operations_not_available_in_share_mode(FileGuid, ShareId, Errors) ->
    ShareFileGuid = file_id:guid_to_share_guid(FileGuid, ShareId),
    {ok, ShareFileObjectId} = file_id:guid_to_objectid(ShareFileGuid),

    [
        % Errors in share mode:
        % - rest: thrown by middleware operation_supported check (rest_handler
        %   changes scope to public when using share object id)
        % - gs: scope is left intact (in contrast to rest) but client is changed
        %   to ?GUEST. Then it fails middleware auth checks (whether user belongs
        %   to space or has some space privileges)
        {bad_id, ShareFileObjectId, {rest, ?ERROR_NOT_SUPPORTED}},
        {bad_id, ShareFileGuid, {gs, ?ERROR_UNAUTHORIZED}}

        | Errors
    ].


%% @private
translate_membership(?NONE_MEMBERSHIP) -> <<"none">>;
translate_membership(?DIRECT_MEMBERSHIP) -> <<"direct">>;
translate_membership(?ANCESTOR_MEMBERSHIP) -> <<"ancestor">>;
translate_membership(?DIRECT_AND_ANCESTOR_MEMBERSHIP) -> <<"directAndAncestor">>.
