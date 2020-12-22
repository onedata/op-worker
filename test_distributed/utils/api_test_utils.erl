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
-include("modules/fslogic/file_details.hrl").
-include("modules/fslogic/fslogic_common.hrl").
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
    create_file/4, create_file/5,

    fill_file_with_dummy_data/4,
    fill_file_with_dummy_data/5,
    write_file/5,
    read_file/4,

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
    file_details_to_gs_json/2
]).
-export([
    add_file_id_errors_for_operations_available_in_share_mode/3,
    add_file_id_errors_for_operations_not_available_in_share_mode/3,
    add_cdmi_id_errors_for_operations_not_available_in_share_mode/4,
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
    list_to_binary(rpc:call(Node, oneprovider, get_rest_endpoint, [
        string:trim(filename:join([<<"/">> | PathTokens]), leading, [$/])
    ])).


-spec create_shared_file_in_space_krk() ->
    {file_type(), file_meta:path(), file_id:file_guid(), od_share:id()}.
create_shared_file_in_space_krk() ->
    [P1Node] = oct_background:get_provider_nodes(krakow),

    UserSessId = oct_background:get_user_session_id(user3, krakow),
    SpaceOwnerSessId = oct_background:get_user_session_id(user1, krakow),

    FileType = randomly_choose_file_type_for_test(),
    FilePath = filename:join(["/", ?SPACE_KRK, ?RANDOM_FILE_NAME()]),
    {ok, FileGuid} = create_file(FileType, P1Node, UserSessId, FilePath),
    {ok, ShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessId, {guid, FileGuid}, <<"share">>),

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
    {ok, FileGuid} = create_file(FileType, P1Node, UserSessIdP1, FilePath, Mode),
    {ok, ShareId} = lfm_proxy:create_share(P1Node, SpaceOwnerSessIdP1, {guid, FileGuid}, <<"share">>),

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
    {ok, FileGuid} = create_file(
        FileType, P1Node, UserSessIdP1, FilePath, FileMode
    ),
    FileShares = case randomly_create_share(P1Node, SpaceOwnerSessIdP1, FileGuid) of
        undefined -> [];
        ShareId -> [ShareId]
    end,
    Size = case FileType of
        <<"file">> ->
            RandSize = rand:uniform(20),
            fill_file_with_dummy_data(P1Node, SpaceOwnerSessIdP1, FileGuid, RandSize),
            RandSize;
        _ ->
            0
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
        index_startid = FileName,
        active_permissions_type = case HasAcl of
            true -> acl;
            false -> posix
        end,
        has_metadata = HasMetadata,
        has_direct_qos = HasDirectQos,
        has_eff_qos = HasParentQos orelse HasDirectQos
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


-spec create_file(file_type(), node(), session:id(), file_meta:path()) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(FileType, Node, SessId, Path) ->
    create_file(FileType, Node, SessId, Path, 8#777).


-spec create_file(file_type(), node(), session:id(), file_meta:path(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | {error, term()}.
create_file(<<"file">>, Node, SessId, Path, Mode) ->
    lfm_proxy:create(Node, SessId, Path, Mode);
create_file(<<"dir">>, Node, SessId, Path, Mode) ->
    lfm_proxy:mkdir(Node, SessId, Path, Mode).


-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid(), Size :: non_neg_integer()) ->
    WrittenContent :: binary().
fill_file_with_dummy_data(Node, SessId, FileGuid, Size) ->
    fill_file_with_dummy_data(Node, SessId, FileGuid, 0, Size).


-spec fill_file_with_dummy_data(node(), session:id(), file_id:file_guid(),
    Offset :: non_neg_integer(), Size :: non_neg_integer()) -> WrittenContent :: binary().
fill_file_with_dummy_data(Node, SessId, FileGuid, Offset, Size) ->
    Content = crypto:strong_rand_bytes(Size),
    write_file(Node, SessId, FileGuid, Offset, Content),
    Content.


-spec write_file(node(), session:id(), file_id:file_guid(), Offset :: non_neg_integer(),
    Size :: non_neg_integer()) -> ok.
write_file(Node, SessId, FileGuid, Offset, Content) ->
    {ok, Handle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {guid, FileGuid}, write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, Handle, Offset, Content)),
    ?assertMatch(ok, lfm_proxy:fsync(Node, Handle)),
    ?assertMatch(ok, lfm_proxy:close(Node, Handle)).


-spec read_file(node(), session:id(), file_id:file_guid(), Size :: non_neg_integer()) ->
    Content :: binary().
read_file(Node, SessId, FileGuid, Size) ->
    {ok, ReadHandle} = lfm_proxy:open(Node, SessId, {guid, FileGuid}, read),
    {ok, Content} = lfm_proxy:read(Node, ReadHandle, 0, Size),
    ok = lfm_proxy:close(Node, ReadHandle),
    Content.


-spec share_file_and_sync_file_attrs(node(), session:id(), [node()], file_id:file_guid()) ->
    od_share:id().
share_file_and_sync_file_attrs(CreationNode, SessionId, SyncNodes, FileGuid) ->
    {ok, ShareId} = ?assertMatch(
        {ok, _},
        lfm_proxy:create_share(CreationNode, SessionId, {guid, FileGuid}, <<"share">>),
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
    lfm_proxy:set_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, rdf, Metadata, []);
set_metadata(Node, FileGuid, <<"json">>, Metadata) ->
    lfm_proxy:set_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, json, Metadata, []);
set_metadata(Node, FileGuid, <<"xattrs">>, Metadata) ->
    set_xattrs(Node, FileGuid, Metadata).


-spec get_metadata(node(), file_id:file_guid(), metadata_type()) -> {ok, term()}.
get_metadata(Node, FileGuid, <<"rdf">>) ->
    lfm_proxy:get_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, rdf, [], false);
get_metadata(Node, FileGuid, <<"json">>) ->
    lfm_proxy:get_metadata(Node, ?ROOT_SESS_ID, {guid, FileGuid}, json, [], false);
get_metadata(Node, FileGuid, <<"xattrs">>) ->
    get_xattrs(Node, FileGuid).


-spec set_xattrs(node(), file_id:file_guid(), map()) -> ok.
set_xattrs(Node, FileGuid, Xattrs) ->
    lists:foreach(fun({Key, Val}) ->
        ?assertMatch(ok, lfm_proxy:set_xattr(
            Node, ?ROOT_SESS_ID, {guid, FileGuid}, #xattr{
                name = Key,
                value = Val
            }
        ), ?ATTEMPTS)
    end, maps:to_list(Xattrs)).


-spec get_xattrs(node(), file_id:file_guid()) -> {ok, map()}.
get_xattrs(Node, FileGuid) ->
    FileKey = {guid, FileGuid},

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
            {ok, QosEntryId} = ?assertMatch({ok, _}, lfm_proxy:add_qos_entry(
                RandNode, ?ROOT_SESS_ID, {guid, FileGuid}, Expression, ReplicasNum
            ), ?ATTEMPTS),
            lists:foreach(fun(Node) ->
                ?assertMatch({ok, _}, lfm_proxy:get_qos_entry(Node, ?ROOT_SESS_ID, QosEntryId), ?ATTEMPTS)
            end, Nodes),
            true;
        2 ->
            false
    end.


-spec randomly_set_metadata([node()], file_id:file_guid()) -> Set :: boolean().
randomly_set_metadata(Nodes, FileGuid) ->
    case rand:uniform(2) of
        1 ->
            FileKey = {guid, FileGuid},
            RandNode = lists_utils:random_element(Nodes),
            ?assertMatch(ok, lfm_proxy:set_metadata(
                RandNode, ?ROOT_SESS_ID, FileKey, rdf, ?RDF_METADATA_1, []
            ), ?ATTEMPTS),
            lists:foreach(fun(Node) ->
                ?assertMatch(
                    {ok, _},
                    lfm_proxy:get_metadata(Node, ?ROOT_SESS_ID, FileKey, rdf, [], false),
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
            FileKey = {guid, FileGuid},
            RandNode = lists_utils:random_element(Nodes),
            ?assertMatch(ok, lfm_proxy:set_acl(
                RandNode, ?ROOT_SESS_ID, {guid, FileGuid}, acl:from_json(?OWNER_ONLY_ALLOW_ACL, cdmi)
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
            {ok, ShId} = ?assertMatch({ok, _}, lfm_proxy:create_share(
                Node, SessionId, {guid, FileGuid}, <<"share">>
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
        provider_id = ProviderId
    },
    index_startid = IndexStartId,
    active_permissions_type = ActivePermissionsType,
    has_metadata = HasMetadata,
    has_direct_qos = HasDirectQos,
    has_eff_qos = HasEffQos
}) ->
    {DisplayedType, DisplayedSize} = case Type of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, Size}
    end,

    #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => FileGuid,
        <<"name">> => FileName,
        <<"index">> => IndexStartId,
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode, 8), 3, $0)),
        % For space dir gs returns null as parentId instead of user root dir
        % (gui doesn't know about user root dir)
        <<"parentId">> => case fslogic_uuid:is_space_dir_guid(FileGuid) of
            true -> null;
            false -> ParentGuid
        end,
        <<"mtime">> => MTime,
        <<"type">> => DisplayedType,
        <<"size">> => DisplayedSize,
        <<"shares">> => Shares,
        <<"activePermissionsType">> => atom_to_binary(ActivePermissionsType, utf8),
        <<"providerId">> => ProviderId,
        <<"ownerId">> => OwnerId,
        <<"hasDirectQos">> => HasDirectQos,
        <<"hasEffQos">> => HasEffQos
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
        shares = Shares
    },
    index_startid = IndexStartId,
    active_permissions_type = ActivePermissionsType,
    has_metadata = HasMetadata
}) ->
    {DisplayedType, DisplayedSize} = case Type of
        ?DIRECTORY_TYPE ->
            {<<"dir">>, null};
        _ ->
            {<<"file">>, Size}
    end,
    IsShareRoot = lists:member(ShareId, Shares),

    #{
        <<"hasMetadata">> => HasMetadata,
        <<"guid">> => file_id:guid_to_share_guid(FileGuid, ShareId),
        <<"name">> => FileName,
        <<"index">> => IndexStartId,
        <<"posixPermissions">> => list_to_binary(string:right(integer_to_list(Mode band 2#111, 8), 3, $0)),
        <<"parentId">> => case IsShareRoot of
            true -> null;
            false -> file_id:guid_to_share_guid(ParentGuid, ShareId)
        end,
        <<"mtime">> => MTime,
        <<"type">> => DisplayedType,
        <<"size">> => DisplayedSize,
        <<"shares">> => case IsShareRoot of
            true -> [ShareId];
            false -> []
        end,
        <<"activePermissionsType">> => atom_to_binary(ActivePermissionsType, utf8)
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
    InvalidFileIdErrors = get_invalid_file_id_errors(),

    NonExistentSpaceGuid = file_id:pack_share_guid(<<"InvalidUuid">>, ?NOT_SUPPORTED_SPACE_ID, ShareId),
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
                ProvId = op_test_rpc:get_provider_id(Node),
                ?ERROR_SPACE_NOT_SUPPORTED_BY(ProvId)
            end}
    end,

    SpaceId = file_id:guid_to_space_id(FileGuid),
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
    InvalidFileIdErrors = get_invalid_file_id_errors(),

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
        {<<"fileId">>, <<"InvalidObjectId">>, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},
        {<<"fileId">>, DummyObjectId, ?ERROR_BAD_VALUE_IDENTIFIER(<<"fileId">>)},

        % user has no privileges in non existent space and so he should receive ?ERROR_FORBIDDEN
        {<<"fileId">>, NonExistentSpaceObjectId, ?ERROR_FORBIDDEN},
        {<<"fileId">>, NonExistentSpaceShareObjectId, ?ERROR_FORBIDDEN},
        
        {<<"fileId">>, NonExistentFileObjectId, ?ERROR_POSIX(?ENOENT)},
        
        % operation on shared file is forbidden - it should result in ?EACCES
        {<<"fileId">>, ShareFileObjectId, ?ERROR_POSIX(?EACCES)},
        {<<"fileId">>, NonExistentFileShareObjectId, ?ERROR_POSIX(?EACCES)}
    ],

    add_bad_values_to_data_spec(BadFileIdValues, DataSpec).


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
get_invalid_file_id_errors() ->
    InvalidGuid = <<"InvalidGuid">>,
    {ok, InvalidObjectId} = file_id:guid_to_objectid(InvalidGuid),
    InvalidIdExpError = ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>),

    [
        % Errors thrown by rest_handler, which failed to convert file path/cdmi_id to guid
        {bad_id, <<"/NonExistentPath">>, {rest_with_file_path, ?ERROR_POSIX(?ENOENT)}},
        {bad_id, <<"InvalidObjectId">>, {rest, ?ERROR_BAD_VALUE_IDENTIFIER(<<"id">>)}},

        % Errors thrown by middleware and internal logic
        {bad_id, InvalidObjectId, {rest, InvalidIdExpError}},
        {bad_id, InvalidGuid, {gs, InvalidIdExpError}}
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
