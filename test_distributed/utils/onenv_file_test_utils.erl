%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Utility functions operating on files used in ct tests.
%%% @end
%%%-------------------------------------------------------------------
-module(onenv_file_test_utils).
-author("Bartosz Walkowicz").

-include("onenv_file_test_utils.hrl").
-include_lib("ctool/include/test/test_utils.hrl").


-export([create_and_sync_files/3]).

-type file_selector() :: file_id:file_guid() | oct_background:entity_selector().
-type file_desc() :: #file{} | #dir{}.

-type share_desc() :: #{name := binary(), desc => binary()}.
-type shares_desc() :: non_neg_integer() | [share_desc()].

-export_type([file_selector/0, file_desc/0, share_desc/0, shares_desc/0]).

-define(ATTEMPTS, 30).


%%%===================================================================
%%% API
%%%===================================================================


-spec create_and_sync_files(oct_background:entity_selector(), file_selector(), file_desc()) ->
    map().
create_and_sync_files(UserSelector, ParentSelector, Files) ->
    UserId = oct_background:get_user_id(UserSelector),
    {ParentGuid, SpaceId} = resolve_file(ParentSelector),
    [CreationProvider | SyncProviders] = oct_background:get_space_supporting_providers(
        SpaceId
    ),
    create_files(UserId, ParentGuid, CreationProvider, SyncProviders, Files).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%% @private
-spec create_files(
    od_user:id(),
    file_id:file_guid(),
    oct_background:entity_selector(),
    [oct_background:entity_selector()],
    file_desc()
) ->
    map().
create_files(UserId, ParentGuid, CreationProvider, SyncProviders, #file{
    name = NameOrUndefined,
    mode = FileMode,
    shares = SharesDesc
}) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = lists_utils:random_element(oct_background:get_provider_nodes(CreationProvider)),

    {ok, FileGuid} = create_file(CreationNode, UserSessId, ParentGuid, FileName, FileMode),
    Shares = create_shares(CreationNode, UserSessId, FileGuid, SharesDesc),

    await_sync(SyncProviders, UserId, FileGuid, Shares),
    #{name => FileName, guid => FileGuid, shares => Shares};

create_files(UserId, ParentGuid, CreationProvider, SyncProviders, #dir{
    name = NameOrUndefined,
    mode = FileMode,
    shares = SharesDesc,
    children = ChildrenDesc
}) ->
    FileName = utils:ensure_defined(NameOrUndefined, str_utils:rand_hex(20)),
    UserSessId = oct_background:get_user_session_id(UserId, CreationProvider),
    CreationNode = lists_utils:random_element(oct_background:get_provider_nodes(CreationProvider)),

    {ok, DirGuid} = create_dir(CreationNode, UserSessId, ParentGuid, FileName, FileMode),
    Shares = create_shares(CreationNode, UserSessId, DirGuid, SharesDesc),

    Children = lists_utils:pmap(fun(File) ->
        create_files(UserId, DirGuid, CreationProvider, SyncProviders, File)
    end, ChildrenDesc),

    await_sync(SyncProviders, UserId, DirGuid, Shares),
    #{name => FileName, guid => DirGuid, shares => Shares, children => Children}.


%% @private
-spec create_shares(node(), session:id(), file_id:file_guid(), shares_desc()) ->
    [od_share:id()] | no_return().
create_shares(Node, SessId, FileGuid, SharesDesc0) ->
    SharesDesc1 = case is_integer(SharesDesc0) of
        true -> [#{name => <<"share">>} || _ <- lists:seq(1, SharesDesc0)];
        false -> SharesDesc0
    end,

    lists:reverse(lists:map(fun(ShareDesc) ->
        {ok, ShareId} = ?assertMatch(
            {ok, _},
            lfm_proxy:create_share(
                Node, SessId, {guid, FileGuid},
                maps:get(name, ShareDesc), maps:get(desc, ShareDesc, <<>>)
            ),
            ?ATTEMPTS
        ),
        ShareId
    end, SharesDesc1)).


%% @private
-spec await_sync(oct_background:entity_selector(), od_user:id(), file_id:file_guid(), [od_share:id()]) ->
    ok | no_return().
await_sync(SyncProviders, UserId, FileGuid, Shares) ->
    lists:foreach(fun(SyncProvider) ->
        SessId = oct_background:get_user_session_id(UserId, SyncProvider),
        SyncNode = lists_utils:random_element(oct_background:get_provider_nodes(SyncProvider)),

        ?assertMatch(
            {ok, #file_attr{shares = Shares}},
            file_test_utils:get_attrs(SyncNode, SessId, FileGuid),
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
-spec resolve_file(file_selector()) -> {file_id:file_guid(), od_space:id()}.
resolve_file(FileSelector) ->
    try
        SpaceId = oct_background:get_space_id(FileSelector),
        {fslogic_uuid:spaceid_to_space_dir_guid(SpaceId), SpaceId}
    catch _:_ ->
        {FileSelector, file_id:guid_to_space_id(FileSelector)}
    end.
