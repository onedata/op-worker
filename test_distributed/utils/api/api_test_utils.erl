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

-include("api/api_test_runner.hrl").
-include("api/api_file_metadata_test.hrl").
-include("modules/dataset/dataset.hrl").
-include("modules/datastore/qos.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("proto/oneclient/common_messages.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include("test_utils/initializer.hrl").



-export([
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
    get_metadata/3,
    get_xattrs/2,

    randomly_add_qos/4,
    randomly_create_share/3
]).

-type file_type() :: binary(). % <<"file">> | <<"dir">>
-type metadata_type() :: binary().  % <<"rdf">> | <<"json">> | <<"xattrs">>.

-export_type([file_type/0, metadata_type/0]).


-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


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
    {file_type(), file_meta:path(), file_id:file_guid(), #file_attr{}}.
create_file_in_space_krk_par_with_additional_metadata(ParentPath, HasParentQos, FileName) ->
    FileType = randomly_choose_file_type_for_test(false),
    create_file_in_space_krk_par_with_additional_metadata(ParentPath, HasParentQos, FileType, FileName).


-spec create_file_in_space_krk_par_with_additional_metadata(
    file_meta:path(),
    boolean(),
    file_type(),
    file_meta:name()
) ->
    {file_type(), file_meta:path(), file_id:file_guid(), #file_attr{}}.
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
            0
    end,
    
    file_test_utils:await_sync(Nodes, FileGuid),
    
    {ok, FileAttr} = ?assertMatch(
        {ok, #file_attr{size = Size, shares = FileShares}},
        lfm_proxy:stat(P2Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), ?API_FILE_ATTRS),
        ?ATTEMPTS
    ),

    HasDirectQos = randomly_add_qos(Nodes, FileGuid, <<"key=value2">>, 2),
    {HasMetadata, JsonMetadata} = randomly_set_metadata(Nodes, FileGuid),
    HasAcl = randomly_set_acl(Nodes, FileGuid),

    FinalFileAttr = FileAttr#file_attr{
        active_permissions_type = case HasAcl of
            true -> acl;
            false -> posix
        end,
        acl = case HasAcl of
            true -> acl:from_json(?OWNER_ONLY_ALLOW_ACL, cdmi);
            false -> []
        end,
        eff_qos_inheritance_path = case {HasDirectQos, HasParentQos} of
            {true, true} -> ?direct_and_ancestor_inheritance_path;
            {true, _} -> ?direct_inheritance_path;
            {_, true} -> ?ancestor_inheritance;
            _ -> ?none_inheritance_path
        end,
        qos_status = case HasDirectQos orelse HasParentQos of
            true -> ?IMPOSSIBLE_QOS_STATUS;
            false -> undefined
        end,
        has_custom_metadata = HasMetadata,
        has_json_metadata = JsonMetadata =/= undefined,
        json_metadata = JsonMetadata
    },

    {FileType, FilePath, FileGuid, FinalFileAttr}.


-spec randomly_choose_file_type_for_test() -> file_type().
randomly_choose_file_type_for_test() ->
    randomly_choose_file_type_for_test(true).


-spec randomly_choose_file_type_for_test(boolean()) -> file_type().
randomly_choose_file_type_for_test(LogSelectedFileType) ->
    FileType = ?RANDOM_FILE_TYPE(),
    LogSelectedFileType andalso ct:pal("Chosen file type for test: ~ts", [FileType]),
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


-spec get_metadata(node(), file_id:file_guid(), metadata_type()) -> {ok, term()}.
get_metadata(Node, FileGuid, <<"rdf">>) ->
    opt_file_metadata:get_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), rdf, [], false);
get_metadata(Node, FileGuid, <<"json">>) ->
    opt_file_metadata:get_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), json, [], false);
get_metadata(Node, FileGuid, <<"xattrs">>) ->
    get_xattrs(Node, FileGuid).


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


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec set_metadata(node(), file_id:file_guid(), metadata_type(), term()) -> ok.
set_metadata(Node, FileGuid, <<"rdf">>, Metadata) ->
    opt_file_metadata:set_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), rdf, Metadata, []);
set_metadata(Node, FileGuid, <<"json">>, Metadata) ->
    opt_file_metadata:set_custom_metadata(Node, ?ROOT_SESS_ID, ?FILE_REF(FileGuid), json, Metadata, []);
set_metadata(Node, FileGuid, <<"xattrs">>, Metadata) ->
    set_xattrs(Node, FileGuid, Metadata).


%% @private
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


%% @private
-spec randomly_set_metadata([node()], file_id:file_guid()) ->
    {HasCustomMetadata :: boolean(), JsonMetadata :: json_utils:json_term()}.
randomly_set_metadata(Nodes, FileGuid) ->
    case rand:uniform(3) of
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
            {true, undefined};
        2 ->
            JsonMetadata = lists_utils:random_element([null, ?JSON_METADATA_1, ?JSON_METADATA_2]), % NOTE: `null` is a valid json metadata
            FileKey = ?FILE_REF(FileGuid),
            RandNode = lists_utils:random_element(Nodes),
            ?assertMatch(ok, opt_file_metadata:set_custom_metadata(
                RandNode, ?ROOT_SESS_ID, FileKey, json, JsonMetadata, []
            ), ?ATTEMPTS),
            lists:foreach(fun(Node) ->
                ?assertMatch(
                    {ok, _},
                    opt_file_metadata:get_custom_metadata(Node, ?ROOT_SESS_ID, FileKey, json, [], false),
                    ?ATTEMPTS
                )
            end, Nodes),
            {true, JsonMetadata};
        3 ->
            {false, undefined}
    end.


%% @private
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
