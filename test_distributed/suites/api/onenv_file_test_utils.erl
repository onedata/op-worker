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
%%% 1) flag to ensure files are created on storage
%%% 2) fill file with random content and await file location sync
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_file_test_utils).
-author("Bartosz Walkowicz").

-include("onenv_file_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([create_and_sync_file_tree/3]).

-type share_spec() :: #share_spec{}.

-type file_selector() :: file_id:file_guid() | oct_background:entity_selector().
-type file_spec() :: #file_spec{} | #dir_spec{}.

-type object() :: #object{}.

-export_type([share_spec/0, file_selector/0, file_spec/0, object/0]).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_and_sync_file_tree(oct_background:entity_selector(), file_selector(), file_spec()) ->
    map().
create_and_sync_file_tree(UserSelector, ParentSelector, FileDesc) ->
    UserId = oct_background:get_user_id(UserSelector),
    {ParentGuid, SpaceId} = resolve_file(ParentSelector),
    [CreationProvider | SyncProviders] = oct_background:get_space_supporting_providers(
        SpaceId
    ),

    FileInfo = create_file_tree(UserId, ParentGuid, CreationProvider, FileDesc),
    await_sync(SyncProviders, UserId, FileInfo),

    FileInfo.


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_file_tree(
    od_user:id(),
    file_id:file_guid(),
    oct_background:entity_selector(),
    file_spec()
) ->
    object().
create_file_tree(UserId, ParentGuid, CreationProvider, #file_spec{
    name = NameOrUndefined,
    mode = FileMode,
    shares = ShareSpecs
}) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = lists_utils:random_element(oct_background:get_provider_nodes(CreationProvider)),

    {ok, FileGuid} = create_file(CreationNode, UserSessId, ParentGuid, FileName, FileMode),

    #object{
        guid = FileGuid,
        name = FileName,
        type = ?REGULAR_FILE_TYPE,
        mode = FileMode,
        shares = create_shares(CreationNode, UserSessId, FileGuid, ShareSpecs),
        children = undefined
    };

create_file_tree(UserId, ParentGuid, CreationProvider, #dir_spec{
    name = NameOrUndefined,
    mode = DirMode,
    shares = ShareSpecs,
    children = ChildrenDesc
}) ->
    DirName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = lists_utils:random_element(oct_background:get_provider_nodes(CreationProvider)),

    {ok, DirGuid} = create_dir(CreationNode, UserSessId, ParentGuid, DirName, DirMode),

    #object{
        guid = DirGuid,
        name = DirName,
        type = ?DIRECTORY_TYPE,
        mode = DirMode,
        shares = create_shares(CreationNode, UserSessId, DirGuid, ShareSpecs),
        children = lists_utils:pmap(fun(File) ->
            create_file_tree(UserId, DirGuid, CreationProvider, File)
        end, ChildrenDesc)
    }.


%% @private
-spec create_shares(node(), session:id(), file_id:file_guid(), [share_spec()]) ->
    [od_share:id()] | no_return().
create_shares(Node, SessId, FileGuid, ShareSpecs) ->
    lists:sort(lists:map(fun(#share_spec{name = Name, description = Description}) ->
        {ok, ShareId} = ?assertMatch(
            {ok, _},
            lfm_proxy:create_share(Node, SessId, {guid, FileGuid}, Name, Description),
            ?ATTEMPTS
        ),
        ShareId
    end, ShareSpecs)).


%% @private
-spec await_sync([oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_sync(SyncProviders, UserId, #object{type = ?REGULAR_FILE_TYPE} = Object) ->
    await_file_attr_sync(SyncProviders, UserId, Object);

await_sync(SyncProviders, UserId, #object{
    guid = DirGuid,
    type = ?DIRECTORY_TYPE,
    children = Children
} = Object) ->
    await_file_attr_sync(SyncProviders, UserId, Object#object{children = undefined}),

    ChildGuids = lists:sort(lists_utils:pforeach(fun(#object{guid = ChildGuid} = Child) ->
        await_sync(SyncProviders, UserId, Child),
        ChildGuid
    end, Children)),

    await_file_links_sync(SyncProviders, UserId, DirGuid, ChildGuids).


%% @private
-spec await_file_attr_sync([oct_background:entity_selector()], od_user:id(), object()) ->
    ok | no_return().
await_file_attr_sync(SyncProviders, UserId, #object{guid = Guid} = Object) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = lists_utils:random_element(oct_background:get_provider_nodes(SyncProvider)),
        ?assertMatch({ok, Object}, get_object(SyncNode, SessId, Guid), ?ATTEMPTS)
    end, SyncProviders).


%% @private
-spec get_object(oct_background:entity_selector(), session:id(), file_id:file_guid()) ->
    {ok, object()} | {error, term()}.
get_object(Node, SessId, Guid) ->
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
-spec await_file_links_sync(
    [oct_background:entity_selector()], od_user:id(), file_id:file_guid(), [file_id:file_guid()]
) ->
    ok | no_return().
await_file_links_sync(SyncProviders, UserId, DirGuid, ExpChildGuids) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = lists_utils:random_element(oct_background:get_provider_nodes(SyncProvider)),
        ?assertMatch(
            {ok, ExpChildGuids},
            ls(SyncNode, SessId, DirGuid, 0, length(ExpChildGuids)),
            ?ATTEMPTS
        )
    end, SyncProviders).


%% @private
-spec ls(
    oct_background:entity_selector(), session:id(), file_id:file_guid(), non_neg_integer(), non_neg_integer()
) ->
    {ok, [file_id:file_guid()]} | {error, term()}.
ls(Node, SessId, Guid, Offset, Size) ->
    case lfm_proxy:get_children(Node, SessId, {guid, Guid}, Offset, Size) of
        {ok, Files} ->
            {ok, lists:sort(lists:map(fun({Guid, _}) -> Guid end, Files))};
        {error, _} = Error ->
            Error
    end.


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
-spec resolve_file(file_selector()) -> {file_id:file_guid(), od_space:id()}.
resolve_file(FileSelector) ->
    try
        SpaceId = oct_background:get_space_id(FileSelector),
        {fslogic_uuid:spaceid_to_space_dir_guid(SpaceId), SpaceId}
    catch error:{badkeys, _} ->
        {FileSelector, file_id:guid_to_space_id(FileSelector)}
    end.
