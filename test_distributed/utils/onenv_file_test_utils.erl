%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020-2021 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions operating on files used in ct tests.
%%%
%%% FUTURE IMPROVEMENTS:
%%% * flag to ensure files are created on storage
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_file_test_utils).
-author("Bartosz Walkowicz").

-include("onenv_test_utils.hrl").
-include("distribution_assert.hrl").
-include("modules/logical_file_manager/lfm.hrl").
-include("modules/fslogic/file_details.hrl").
-include_lib("ctool/include/test/test_utils.hrl").
-include_lib("ctool/include/errors.hrl").


-export([
    resolve_file/1,
    create_and_sync_file_tree/3, create_and_sync_file_tree/4,
    mv_and_sync_file/3, rm_and_sync_file/2, await_file_metadata_sync/3
]).
-export([get_object_attributes/3]).
-type share_spec() :: #share_spec{}.

-type space_selector() :: oct_background:entity_selector().
-type object_selector() :: file_id:file_guid() | space_selector().
-type object_spec() :: #file_spec{} | #dir_spec{} | #symlink_spec{} | metadata_spec().

-type object() :: #object{}.
-type metadata_object() :: #metadata_object{}.
-type metadata_spec() :: #metadata_spec{}.

-export_type([share_spec/0, space_selector/0, object_selector/0, object_spec/0, object/0,
    metadata_spec/0, metadata_object/0
]).

-define(LS_SIZE, 1000).

-define(ATTEMPTS, 60).


%%%===================================================================
%%% API
%%%===================================================================


-spec resolve_file(object_selector()) -> {file_id:file_guid(), od_space:id()}.
resolve_file(FileSelector) ->
    try
        SpaceId = oct_background:get_space_id(FileSelector),
        {fslogic_uuid:spaceid_to_space_dir_guid(SpaceId), SpaceId}
    catch error:{badkeys, _} ->
        {FileSelector, file_id:guid_to_space_id(FileSelector)}
    end.


-spec create_and_sync_file_tree(
    oct_background:entity_selector(),
    object_selector(),
    object_spec() | [object_spec()]
) ->
    object() | [object()].
create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc) ->
    {_ParentGuid, SpaceId} = resolve_file(ParentSelector),
    [CreationProvider | _] = oct_background:get_space_supporting_providers(
        SpaceId
    ),
    create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProvider).


-spec create_and_sync_file_tree(
    oct_background:entity_selector(),
    object_selector(),
    object_spec() | [object_spec()],
    oct_background:entity_selector()
) ->
    object() | [object()].
create_and_sync_file_tree(UserSelector, ParentSelector, FilesDesc, CreationProviderSelector) when is_list(FilesDesc) ->
    lists_utils:pmap(fun(FileDesc) ->
        create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProviderSelector)
    end, FilesDesc);

create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProviderSelector) ->
    UserId = oct_background:get_user_id(UserSelector),
    {ParentGuid, SpaceId} = resolve_file(ParentSelector),
    SupportingProviders = oct_background:get_space_supporting_providers(SpaceId),
    CreationProvider = oct_background:get_provider_id(CreationProviderSelector),
    SyncProviders = SupportingProviders -- [CreationProvider],

    FileTree = create_file_tree(UserId, ParentGuid, CreationProvider, FileDesc),
    await_sync(CreationProvider, SyncProviders, UserId, FileTree),
    await_parent_links_sync(SyncProviders, UserId, ParentGuid, FileTree),

    FileTree.


-spec mv_and_sync_file(oct_background:entity_selector(), object_selector(), file_meta:path()) ->
    ok.
mv_and_sync_file(UserSelector, FileSelector, DstPath) ->
    UserId = oct_background:get_user_id(UserSelector),
    {FileGuid, SpaceId} = resolve_file(FileSelector),
    [MvProvider | RestProviders] = lists_utils:shuffle(oct_background:get_space_supporting_providers(
        SpaceId
    )),

    mv_file(UserId, FileGuid, DstPath, MvProvider),

    lists:foreach(fun(Provider) ->
        Node = ?OCT_RAND_OP_NODE(Provider),
        UserSessId = oct_background:get_user_session_id(UserId, Provider),
        ?assertEqual({ok, DstPath}, lfm_proxy:get_file_path(Node, UserSessId, FileGuid), ?ATTEMPTS)
    end, RestProviders).


-spec rm_and_sync_file(oct_background:entity_selector(), object_selector()) ->
    ok.
rm_and_sync_file(UserSelector, FileSelector) ->
    UserId = oct_background:get_user_id(UserSelector),
    {FileGuid, SpaceId} = resolve_file(FileSelector),
    [RmProvider | RestProviders] = lists_utils:shuffle(oct_background:get_space_supporting_providers(
        SpaceId
    )),

    rm_file(UserId, FileGuid, RmProvider),

    lists:foreach(fun(Provider) ->
        Node = ?OCT_RAND_OP_NODE(Provider),
        UserSessId = oct_background:get_user_session_id(UserId, Provider),
        ?assertEqual({error, ?ENOENT}, lfm_proxy:stat(Node, UserSessId, ?FILE_REF(FileGuid)), ?ATTEMPTS)
    end, RestProviders).


-spec get_object_attributes(oct_background:entity_selector(), session:id(), file_id:file_guid()) ->
    {ok, object()} | {error, term()}.
get_object_attributes(Node, SessId, Guid) ->
    case file_test_utils:get_attrs(Node, SessId, Guid) of
        {ok, #file_attr{guid = Guid, name = Name, type = Type, mode = Mode, shares = Shares}} ->
            {ok, #object{
                guid = Guid, name = Name,
                type = Type, mode = Mode,
                shares = lists:sort(Shares)
            }};
        {error, _} = Error ->
            Error
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_file_tree(
    od_user:id(),
    file_id:file_guid(),
    oct_background:entity_selector(),
    object_spec()
) ->
    object().
create_file_tree(UserId, ParentGuid, CreationProvider, #file_spec{
    name = NameOrUndefined,
    mode = FileMode,
    shares = ShareSpecs,
    dataset = DatasetSpec,
    content = Content,
    metadata = MetadataSpec
}) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),

    {ok, FileGuid} = create_file(CreationNode, UserSessId, ParentGuid, FileName, FileMode),
    Content /= <<>> andalso write_file(CreationNode, UserSessId, FileGuid, Content),
    MetadataObj = create_metadata(CreationNode, UserSessId, FileGuid, MetadataSpec),

    DatasetObj = onenv_dataset_test_utils:set_up_dataset(
        CreationProvider, UserId, FileGuid, DatasetSpec
    ),

    #object{
        guid = FileGuid,
        name = FileName,
        type = ?REGULAR_FILE_TYPE,
        mode = FileMode,
        shares = create_shares(CreationProvider, UserSessId, FileGuid, ShareSpecs),
        dataset = DatasetObj,
        content = Content,
        children = undefined,
        metadata = MetadataObj
    };

create_file_tree(UserId, ParentGuid, CreationProvider, #symlink_spec{
    name = NameOrUndefined,
    shares = ShareSpecs,
    symlink_value = SymlinkValue,
    dataset = DatasetSpec
}) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = lists_utils:random_element(oct_background:get_provider_nodes(CreationProvider)),

    {ok, #file_attr{guid = SymlinkGuid}} = create_symlink(CreationNode, UserSessId, ParentGuid, FileName, SymlinkValue),

    DatasetObj = onenv_dataset_test_utils:set_up_dataset(
        CreationProvider, UserId, SymlinkGuid, DatasetSpec
    ),

    #object{
        guid = SymlinkGuid,
        name = FileName,
        type = ?SYMLINK_TYPE,
        shares = create_shares(CreationProvider, UserSessId, SymlinkGuid, ShareSpecs),
        children = undefined,
        content = undefined,
        dataset = DatasetObj,
        mode = ?DEFAULT_SYMLINK_PERMS,
        symlink_value = SymlinkValue
    };

create_file_tree(UserId, ParentGuid, CreationProvider, #dir_spec{
    name = NameOrUndefined,
    mode = DirMode,
    shares = ShareSpecs,
    dataset = DatasetSpec,
    children = ChildrenSpec,
    metadata = MetadataSpec
}) ->
    DirName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),

    {ok, DirGuid} = create_dir(CreationNode, UserSessId, ParentGuid, DirName, DirMode),

    Children = lists_utils:pmap(fun(File) ->
        create_file_tree(UserId, DirGuid, CreationProvider, File)
    end, ChildrenSpec),
    MetadataObj = create_metadata(CreationNode, UserSessId, DirGuid, MetadataSpec),
    
    DatasetObj = onenv_dataset_test_utils:set_up_dataset(
        CreationProvider, UserId, DirGuid, DatasetSpec
    ),
    
    #object{
        guid = DirGuid,
        name = DirName,
        type = ?DIRECTORY_TYPE,
        mode = DirMode,
        shares = create_shares(CreationProvider, UserSessId, DirGuid, ShareSpecs),
        dataset = DatasetObj,
        children = Children,
        metadata = MetadataObj
    }.


%% @private
-spec create_shares(oct_background:entity_selector(), session:id(), file_id:file_guid(), [share_spec()]) ->
    [od_share:id()] | no_return().
create_shares(CreationProvider, SessId, FileGuid, ShareSpecs) ->
    CreationNode = ?OCT_RAND_OP_NODE(CreationProvider),
    lists:sort(lists:map(fun(#share_spec{name = Name, description = Description}) ->
        {ok, ShareId} = ?assertMatch(
            {ok, _},
            opt_shares:create(CreationNode, SessId, ?FILE_REF(FileGuid), Name, Description),
            ?ATTEMPTS
        ),
        ShareId
    end, ShareSpecs)).


%% @private
-spec await_sync(oct_background:entity_selector(), [oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_sync(CreationProvider, SyncProviders, UserId, #object{
    type = ?REGULAR_FILE_TYPE,
    dataset = DatasetObj
} = Object) ->
    await_file_attr_sync(SyncProviders, UserId, Object),
    await_file_metadata_sync(SyncProviders, UserId, Object),
    onenv_dataset_test_utils:await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj),
    await_file_distribution_sync(CreationProvider, SyncProviders, UserId, Object);

await_sync(CreationProvider, SyncProviders, UserId, #object{
    type = ?SYMLINK_TYPE,
    dataset = DatasetObj
} = Object) ->
    % file_attr construction uses file_meta document, so this checks symlink value synchronization
    await_file_attr_sync(SyncProviders, UserId, Object),
    onenv_dataset_test_utils:await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj);

await_sync(CreationProvider, SyncProviders, UserId, #object{
    guid = DirGuid,
    type = ?DIRECTORY_TYPE,
    dataset = DatasetObj,
    children = Children
} = Object) ->
    await_file_attr_sync(SyncProviders, UserId, Object#object{children = undefined}),
    onenv_dataset_test_utils:await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj),

    ExpChildrenList = lists:keysort(2, lists_utils:pmap(fun(#object{guid = ChildGuid, name = ChildName} = Child) ->
        await_sync(CreationProvider, SyncProviders, UserId, Child),
        {ChildGuid, ChildName}
    end, Children)),

    await_dir_links_sync(SyncProviders, UserId, DirGuid, ExpChildrenList).


%% @private
-spec await_file_attr_sync([oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_file_attr_sync(SyncProviders, UserId, #object{guid = Guid} = Object) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        ExpObjectAttrs = Object#object{
            dataset = undefined,
            content = undefined,
            children = undefined,
            symlink_value = undefined,
            metadata = undefined
        },
        ?assertEqual({ok, ExpObjectAttrs}, get_object_attributes(SyncNode, SessId, Guid), ?ATTEMPTS)
    end, SyncProviders).


-spec await_file_metadata_sync([oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_file_metadata_sync(_SyncProviders, _UserId, #object{metadata = undefined}) ->
    ok;
await_file_metadata_sync(SyncProviders, UserId, #object{guid = Guid, metadata = #metadata_object{
    json = ExpJson,
    rdf = ExpRdf,
    xattrs = ExpXattrs
}}) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),

        await_json_metadata_sync(SyncNode, SessId, Guid, ExpJson),
        await_rdf_metadata_sync(SyncNode, SessId, Guid, ExpRdf),
        await_xattrs_sync(SyncNode, SessId, Guid, ExpXattrs)

    end, SyncProviders).


%% @private
-spec await_json_metadata_sync(node(), session:id(), file_id:file_guid(), undefined | json_utils:json_term()) -> ok.
await_json_metadata_sync(_SyncNode, _SessId, _FileGuid, undefined) ->
    ok;
await_json_metadata_sync(SyncNode, SessId, FileGuid, ExpJson) ->
    ?assertEqual({ok, ExpJson}, lfm_proxy:get_metadata(SyncNode, SessId, ?FILE_REF(FileGuid), json, [], false), ?ATTEMPTS),
    ok.


%% @private
-spec await_rdf_metadata_sync(node(), session:id(), file_id:file_guid(), undefined | binary()) -> ok.
await_rdf_metadata_sync(_SyncNode, _SessId, _FileGuid, undefined) ->
    ok;
await_rdf_metadata_sync(SyncNode, SessId, FileGuid, ExpRdf) ->
    ?assertEqual({ok, ExpRdf}, lfm_proxy:get_metadata(SyncNode, SessId, ?FILE_REF(FileGuid), rdf, [], false), ?ATTEMPTS),
    ok.


%% @private
-spec await_xattrs_sync(node(), session:id(), file_id:file_guid(), undefined | json_utils:json_term()) -> ok.
await_xattrs_sync(_SyncNode, _SessId, _FileGuid, undefined) ->
    ok;
await_xattrs_sync(SyncNode, SessId, FileGuid, ExpXattrs) ->
    ExpXattrsNamesSorted = lists:sort(maps:keys(ExpXattrs)),
    ListXattr = fun() ->
        case lfm_proxy:list_xattr(SyncNode, SessId, ?FILE_REF(FileGuid), false, false) of
            {ok, XattrNames} -> {ok, lists:sort(XattrNames)};
            Other -> Other
        end
    end,
    ?assertEqual({ok, ExpXattrsNamesSorted}, ListXattr(), ?ATTEMPTS),

    maps:fold(fun(XattrName, XattrValue, _) ->
        ?assertMatch({ok, #xattr{name = XattrName, value = XattrValue}},
            lfm_proxy:get_xattr(SyncNode, SessId, ?FILE_REF(FileGuid), XattrName), ?ATTEMPTS)
    end, undefined, ExpXattrs),
    ok.


%% @private
-spec await_file_distribution_sync(
    oct_background:entity_selector(),
    [oct_background:entity_selector()],
    od_user:id(),
    object()
) ->
    ok | no_return().
await_file_distribution_sync(_, _, _, #object{type = ?DIRECTORY_TYPE}) ->
    ok;
await_file_distribution_sync(CreationProvider, SyncProviders, UserId, #object{
    type = ?REGULAR_FILE_TYPE, guid = Guid, content = Content
}) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        ?assertDistribution(SyncNode, SessId, ?DIST(CreationProvider, byte_size(Content)), Guid, ?ATTEMPTS)
    end, SyncProviders).


%% @private
-spec await_dir_links_sync(
    [oct_background:entity_selector()],
    od_user:id(),
    file_id:file_guid(),
    [{file_meta:name(), file_id:file_guid()}]
) ->
    ok | no_return().
await_dir_links_sync(SyncProviders, UserId, DirGuid, ExpChildrenList) ->
    lists:foreach(fun(SyncProvider) ->
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        ?assertEqual({ok, ExpChildrenList}, ls(SyncNode, SessId, DirGuid), ?ATTEMPTS)
    end, SyncProviders).


%% @private
-spec ls(oct_background:entity_selector(), session:id(), file_id:file_guid()) ->
    {ok, [{file_id:file_guid(), file_meta:name()}]} | {error, term()}.
ls(Node, SessId, Guid) ->
    ls(Node, SessId, Guid, <<>>, []).


%% @private
-spec ls(
    node(),
    session:id(),
    file_id:file_guid(),
    file_meta:list_token(),
    [{file_id:file_guid(), file_meta:name()}]
) ->
    {ok, [{file_meta:name(), file_id:file_guid()}]} | {error, term()}.
ls(Node, SessId, Guid, Token, ChildEntriesAcc) ->
    case lfm_proxy:get_children(Node, SessId, ?FILE_REF(Guid), #{token => Token, size => ?LS_SIZE}) of
        {ok, Children, ListExtendedInfo} ->
            AllChildEntries = ChildEntriesAcc ++ Children,

            case maps:get(is_last, ListExtendedInfo) of
                true -> {ok, AllChildEntries};
                false -> ls(Node, SessId, Guid, maps:get(token, ListExtendedInfo), AllChildEntries)
            end;
        Error ->
            Error
    end.


%% @private
-spec await_parent_links_sync(
    [oct_background:entity_selector()],
    od_user:id(),
    file_id:file_guid(),
    object()
) ->
    ok | no_return().
await_parent_links_sync(SyncProviders, UserId, ParentGuid, #object{
    guid = ChildGuid,
    name = ChildName
}) ->
    lists:foreach(fun(SyncProvider) ->
        SyncNode = ?OCT_RAND_OP_NODE(SyncProvider),
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),

        ?assertMatch(
            {ok, #file_attr{guid = ChildGuid}},
            lfm_proxy:get_child_attr(SyncNode, SessId, ParentGuid, ChildName),
            ?ATTEMPTS
        )
    end, SyncProviders).


%% @private
-spec create_file(node(), session:id(), file_id:file_guid(), file_meta:name(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | no_return().
create_file(Node, SessId, ParentGuid, FileName, FileMode) ->
    ?assertMatch({ok, _}, lfm_proxy:create(Node, SessId, ParentGuid, FileName, FileMode)).


%% @private
-spec create_dir(node(), session:id(), file_id:file_guid(), file_meta:name(), file_meta:mode()) ->
    {ok, file_id:file_guid()} | no_return().
create_dir(Node, SessId, ParentGuid, FileName, FileMode) ->
    ?assertMatch({ok, _}, lfm_proxy:mkdir(Node, SessId, ParentGuid, FileName, FileMode)).


%% @private
-spec create_symlink(node(), session:id(), file_id:file_guid(), file_meta:name(), file_meta_symlinks:symlink()) ->
    {ok, #file_attr{}} | no_return().
create_symlink(Node, SessId, ParentGuid, FileName, SymlinkValue) ->
    ?assertMatch({ok, _}, lfm_proxy:make_symlink(Node, SessId, ?FILE_REF(ParentGuid), FileName, SymlinkValue)).


%% @private
-spec write_file(node(), session:id(), file_id:file_guid(), binary()) ->
    ok | no_return().
write_file(Node, SessId, FileGuid, Content) ->
    {ok, FileHandle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, ?FILE_REF(FileGuid), write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, FileHandle, 0, Content)),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).


%% @private
-spec create_metadata(node(), session:id(), file_id:file_guid(), #metadata_spec{}) ->
    ok | no_return().
create_metadata(Node, SessId, FileGuid, #metadata_spec{
    json = Json,
    rdf = Rdf,
    xattrs = Xattrs
}) ->
    Json /= undefined andalso create_json_metadata(Node, SessId, FileGuid, Json),
    Rdf /= undefined andalso create_rdf_metadata(Node, SessId, FileGuid, Rdf),
    Xattrs /= undefined andalso create_xattrs(Node, SessId, FileGuid, Xattrs),
    
    #metadata_object{
        json = Json,
        rdf = Rdf,
        xattrs = Xattrs
    }.


%% @private
-spec create_json_metadata(node(), session:id(), file_id:file_guid(), json_utils:json_term()) -> ok.
create_json_metadata(Node, SessionId, FileGuid, Json) ->
    ?assertEqual(ok, lfm_proxy:set_metadata(Node, SessionId, ?FILE_REF(FileGuid), json, Json, [])).


%% @private
-spec create_rdf_metadata(node(), session:id(), file_id:file_guid(), binary()) -> ok.
create_rdf_metadata(Node, SessionId, FileGuid, Rdf) ->
    ?assertEqual(ok, lfm_proxy:set_metadata(Node, SessionId, ?FILE_REF(FileGuid), rdf, Rdf, [])).


%% @private
-spec create_xattrs(node(), session:id(), file_id:file_guid(), json_utils:json_term()) -> ok.
create_xattrs(Node, SessionId, FileGuid, Xattrs) ->
    maps:fold(fun(XattrKey, XattrValue, _) ->
        ?assertEqual(ok, lfm_proxy:set_xattr(Node, SessionId, ?FILE_REF(FileGuid), #xattr{
            name = XattrKey, value = XattrValue
        }))
    end, undefined, Xattrs),
    ok.


    %% @private
-spec mv_file(od_user:id(), file_id:file_guid(), file_meta:path(), oct_background:entity_selector()) ->
    ok.
mv_file(UserId, FileGuid, DstPath, MvProvider) ->
    MvNode = ?OCT_RAND_OP_NODE(MvProvider),
    UserSessId = oct_background:get_user_session_id(UserId, MvProvider),

    ?assertMatch({ok, _}, lfm_proxy:mv(MvNode, UserSessId, ?FILE_REF(FileGuid), DstPath)),
    ok.


%% @private
-spec rm_file(od_user:id(), file_id:file_guid(), oct_background:entity_selector()) ->
    ok.
rm_file(UserId, FileGuid, RmProvider) ->
    RmNode = ?OCT_RAND_OP_NODE(RmProvider),
    UserSessId = oct_background:get_user_session_id(UserId, RmProvider),

    ?assertMatch(ok, lfm_proxy:unlink(RmNode, UserSessId, ?FILE_REF(FileGuid))).