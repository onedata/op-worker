%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2020 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This is a helper module for luma_spaces_cache module.
%%% It encapsulates #luma_space{} record, which stores
%%% POSIX-compatible credentials for given space.
%%%
%%% This record has 4 fields:
%%%  * storage_uid - this field is used as uid field in luma:storage_credentials()
%%%                  on POSIX-compatible storages for virtual SpaceOwner
%%%  * storage_gid - this field is used as gid field in luma:storage_credentials()
%%%                  on POSIX-compatible storages for ALL users in given space
%%%  * display_uid - this field is used as uid field in luma:display_credentials()
%%%                  for virtual SpaceOwner
%%%  * display_gid - this field is used as gid field in luma:display_credentials()
%%%                  for ALL users in given space

%%% For more info please read the docs of luma.erl and
%%% luma_spaces_cache.erl modules.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_space_defaults).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([new/4, get_storage_uid/1, get_storage_gid/1, get_display_uid/1, get_display_gid/1]).

-record(luma_space_defaults, {
    display_uid :: undefined | luma:uid(),
    display_gid :: undefined | luma:gid(),
    storage_uid :: undefined | luma:uid(),
    storage_gid :: undefined | luma:gid()
}).

-type defaults() ::  #luma_space_defaults{}.
-export_type([defaults/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(luma:space_mapping(), luma:space_mapping(), od_space:id(), storage:data()) ->
    defaults().
new(DefaultPosixCredentials, DisplayCredentials, SpaceId, Storage) ->
    LumaSpace0 = #luma_space_defaults{
        storage_uid = maps:get(<<"uid">>, DefaultPosixCredentials, undefined),
        storage_gid = maps:get(<<"gid">>, DefaultPosixCredentials, undefined),
        display_uid = maps:get(<<"uid">>, DisplayCredentials, undefined),
        display_gid = maps:get(<<"gid">>, DisplayCredentials, undefined)
    },
    ensure_required_fields_defined(LumaSpace0, SpaceId, Storage, storage:is_posix_compatible(Storage)).

-spec get_storage_uid(defaults()) -> luma:uid().
get_storage_uid(#luma_space_defaults{storage_uid = StorageUid}) ->
    StorageUid.

-spec get_storage_gid(defaults()) -> luma:gid().
get_storage_gid(#luma_space_defaults{storage_gid = StorageGid}) ->
    StorageGid.

-spec get_display_uid(defaults()) -> luma:uid().
get_display_uid(LS = #luma_space_defaults{display_uid = undefined}) ->
    get_storage_uid(LS);
get_display_uid(#luma_space_defaults{display_uid = DisplayUid}) ->
    DisplayUid.

-spec get_display_gid(defaults()) -> luma:gid().
get_display_gid(LS = #luma_space_defaults{display_gid = undefined}) ->
    get_storage_gid(LS);
get_display_gid(#luma_space_defaults{display_gid = DisplayGid}) ->
    DisplayGid.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec ensure_required_fields_defined(defaults(), od_space:id(), storage:data(), IsPosix :: boolean()) -> defaults().
ensure_required_fields_defined(LumaSpace, SpaceId, Storage, true) ->
    ensure_required_fields_on_posix_are_defined(LumaSpace, SpaceId, Storage);
ensure_required_fields_defined(LumaSpace, SpaceId, _Storage, false) ->
    ensure_required_fields_on_non_posix_are_defined(LumaSpace, SpaceId).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that both storage_ fields are defined.
%% It is important only for POSIX compatible storages.
%% If display_ fields are undefined, corresponding storage_ fields
%% will be used in get_display_uid/1 and get_display_gid/1 functions.
%% @end
%%--------------------------------------------------------------------
-spec ensure_required_fields_on_posix_are_defined(defaults(), od_space:id(), storage:data()) -> defaults().
ensure_required_fields_on_posix_are_defined(LS = #luma_space_defaults{
    storage_uid = StorageUid,
    storage_gid = StorageGid
}, _SpaceId, _Storage)
    when StorageUid =/= undefined
    andalso StorageGid =/= undefined
->
    % both storage_ fields are defined
    LS;
ensure_required_fields_on_posix_are_defined(LS = #luma_space_defaults{
    storage_uid = StorageUid,
    storage_gid = StorageGid
}, SpaceId, Storage) ->
    % at least one of storage_ fields is undefined
    {FallbackUid, FallbackGid} = get_mountpoint_credentials(Storage, SpaceId),
    LS#luma_space_defaults{
        storage_uid = ensure_defined(StorageUid, FallbackUid),
        storage_gid = ensure_defined(StorageGid, FallbackGid)
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that both display_ fields are defined.
%% It is important only for POSIX incompatible storages, because
%% storage_ fields are undefined so we cannot fallback to them
%% in get_display_uid/1 and get_display_gid/1 functions.
%% @end
%%--------------------------------------------------------------------
-spec ensure_required_fields_on_non_posix_are_defined(defaults(), od_space:id()) -> defaults().
ensure_required_fields_on_non_posix_are_defined(LS = #luma_space_defaults{
    display_uid = DisplayUid,
    display_gid = DisplayGid
}, _SpaceId)
    when DisplayUid =/= undefined
    andalso DisplayGid =/= undefined
->
    % both display_ fields are defined
    LS;
ensure_required_fields_on_non_posix_are_defined(LS = #luma_space_defaults{
    display_uid = DisplayUid,
    display_gid = DisplayGid
}, SpaceId) ->
    % at least one of display_ fields is undefined
    {FallbackUid, FallbackGid} = generate_credentials(SpaceId),
    LS#luma_space_defaults{
        display_uid = ensure_defined(DisplayUid, FallbackUid),
        display_gid = ensure_defined(DisplayGid, FallbackGid)
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function returns fallback posix credentials which are
%% UID and GID of storage mountpoint directory.
%% @end
%%--------------------------------------------------------------------
-spec get_mountpoint_credentials(storage:data(), od_space:id()) -> {luma:uid(), luma:gid()}.
get_mountpoint_credentials(Storage, SpaceId) ->
    StorageFileCtx = storage_file_ctx:new(?DIRECTORY_SEPARATOR_BINARY, SpaceId, storage:get_id(Storage)),
    {#statbuf{st_uid = Uid, st_gid = Gid}, _} = storage_file_ctx:stat(StorageFileCtx),
    {Uid, Gid}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function returns fallback credentials for POSIX incompatible
%% storages, it generates UID and GID.
%% UID is generated using id of virtual SpaceOwner as hashing base.
%% GID is generated using SpaceId as hashing base
%% @end
%%--------------------------------------------------------------------
-spec generate_credentials(od_space:id()) -> {luma:uid(), luma:gid()}.
generate_credentials(SpaceId) ->
    Uid = luma_utils:generate_uid(?SPACE_OWNER_ID(SpaceId)),
    Gid = luma_utils:generate_gid(SpaceId),
    {Uid, Gid}.


-spec ensure_defined(term(), term()) -> term().
ensure_defined(Value, Default) ->
    utils:ensure_defined(Value, undefined, Default).