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
-include_lib("ctool/include/test/test_utils.hrl").


-export([
    resolve_file/1,
    create_and_sync_file_tree/3, create_and_sync_file_tree/4
]).

-type share_spec() :: #share_spec{}.

-type object_selector() :: file_id:file_guid() | oct_background:entity_selector().
-type object_spec() :: #file_spec{} | #dir_spec{}.

-type object() :: #object{}.

-export_type([share_spec/0, object_selector/0, object_spec/0, object/0]).

-define(LS_SIZE, 1000).

-define(ATTEMPTS, 30).


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


-spec create_and_sync_file_tree(oct_background:entity_selector(), object_selector(), object_spec()) ->
    object().
create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc) ->
    {_ParentGuid, SpaceId} = resolve_file(ParentSelector),
    [CreationProvider | _] = oct_background:get_space_supporting_providers(
        SpaceId
    ),
    create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc, CreationProvider).


-spec create_and_sync_file_tree(
    oct_background:entity_selector(),
    object_selector(),
    object_spec(),
    oct_background:entity_selector()
) ->
    object().
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
    content = Content
}) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = ?RAND_OP_NODE(CreationProvider),

    {ok, FileGuid} = create_file(CreationNode, UserSessId, ParentGuid, FileName, FileMode),
    Content /= <<>> andalso write_file(CreationNode, UserSessId, FileGuid, Content),

    DatasetObj = onenv_dataset_test_utils:prepare_dataset(
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
        children = undefined
    };

create_file_tree(UserId, ParentGuid, CreationProvider, #dir_spec{
    name = NameOrUndefined,
    mode = DirMode,
    shares = ShareSpecs,
    dataset = DatasetSpec,
    children = ChildrenSpec
}) ->
    DirName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = ?RAND_OP_NODE(CreationProvider),

    {ok, DirGuid} = create_dir(CreationNode, UserSessId, ParentGuid, DirName, DirMode),

    Children = lists_utils:pmap(fun(File) ->
        create_file_tree(UserId, DirGuid, CreationProvider, File)
    end, ChildrenSpec),

    DatasetObj = onenv_dataset_test_utils:prepare_dataset(
        CreationProvider, UserId, DirGuid, DatasetSpec
    ),

    #object{
        guid = DirGuid,
        name = DirName,
        type = ?DIRECTORY_TYPE,
        mode = DirMode,
        shares = create_shares(CreationProvider, UserSessId, DirGuid, ShareSpecs),
        dataset = DatasetObj,
        children = Children
    }.


%% @private
-spec create_shares(oct_background:entity_selector(), session:id(), file_id:file_guid(), [share_spec()]) ->
    [od_share:id()] | no_return().
create_shares(CreationProvider, SessId, FileGuid, ShareSpecs) ->
    CreationNode = ?RAND_OP_NODE(CreationProvider),
    lists:sort(lists:map(fun(#share_spec{name = Name, description = Description}) ->
        {ok, ShareId} = ?assertMatch(
            {ok, _},
            lfm_proxy:create_share(CreationNode, SessId, {guid, FileGuid}, Name, Description),
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
    onenv_dataset_test_utils:await_dataset_sync(CreationProvider, SyncProviders, UserId, DatasetObj),
    await_file_distribution_sync(CreationProvider, SyncProviders, UserId, Object);

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
        SyncNode = ?RAND_OP_NODE(SyncProvider),
        ExpObjectAttrs = Object#object{dataset = undefined, content = undefined, children = undefined},
        ?assertEqual({ok, ExpObjectAttrs}, get_object_attributes(SyncNode, SessId, Guid), ?ATTEMPTS)
    end, SyncProviders).


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
        SyncNode = ?RAND_OP_NODE(SyncProvider),
        ?assertDistribution(SyncNode, SessId, ?DIST(CreationProvider, byte_size(Content)), Guid, ?ATTEMPTS)
    end, SyncProviders).


%% @private
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
        SyncNode = ?RAND_OP_NODE(SyncProvider),
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
    case lfm_proxy:get_children(Node, SessId, {guid, Guid}, #{token => Token, size => ?LS_SIZE}) of
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
        SyncNode = ?RAND_OP_NODE(SyncProvider),
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
-spec write_file(node(), session:id(), file_id:file_guid(), binary()) ->
    ok | no_return().
write_file(Node, SessId, FileGuid, Content) ->
    {ok, FileHandle} = ?assertMatch({ok, _}, lfm_proxy:open(Node, SessId, {guid, FileGuid}, write)),
    ?assertMatch({ok, _}, lfm_proxy:write(Node, FileHandle, 0, Content)),
    ?assertMatch(ok, lfm_proxy:close(Node, FileHandle)).
