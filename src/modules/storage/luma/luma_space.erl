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
%%%  * default_uid - this field is used as uid field in luma:storage_credentials()
%%%                  on POSIX-compatible storages for virtual SpaceOwner
%%%  * default_gid - this field is used as gid field in luma:storage_credentials()
%%%                  on POSIX-compatible storages for ALL users in given space
%%%  * display_uid - this field is used as uid field in luma:display_credentials()
%%%                  for virtual SpaceOwner
%%%  * display_gid - this field is used as gid field in luma:display_credentials()
%%%                  for ALL users in given space

%%% For more info please read the docs of luma.erl and
%%% luma_spaces_cache.erl modules.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_space).
-author("Jakub Kudzia").

-include("modules/fslogic/fslogic_common.hrl").

%% API
-export([new/4, get_default_uid/1, get_default_gid/1, get_display_uid/1, get_display_gid/1]).

-record(luma_space, {
    default_uid :: undefined | luma:uid(),
    default_gid :: undefined | luma:gid(),
    display_uid :: undefined | luma:uid(),
    display_gid :: undefined | luma:gid()
}).

-type entry() ::  #luma_space{}.
-export_type([entry/0]).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec new(luma:space_mapping_response(), luma:space_mapping_response(), od_space:id(), storage:data()) ->
    entry().
new(DefaultPosixCredentials, DisplayCredentials, SpaceId, Storage) ->
    LumaSpace0 = #luma_space{
        default_uid = maps:get(<<"uid">>, DefaultPosixCredentials, undefined),
        default_gid = maps:get(<<"gid">>, DefaultPosixCredentials, undefined),
        display_uid = maps:get(<<"uid">>, DisplayCredentials, undefined),
        display_gid = maps:get(<<"gid">>, DisplayCredentials, undefined)
    },
    ensure_all_fields_defined(LumaSpace0, SpaceId, Storage, storage:is_posix_compatible(Storage)).

-spec get_default_uid(entry()) -> luma:uid().
get_default_uid(#luma_space{default_uid = DefaultUid}) ->
    DefaultUid.

-spec get_default_gid(entry()) -> luma:gid().
get_default_gid(#luma_space{default_gid = DefaultGid}) ->
    DefaultGid.

-spec get_display_uid(entry()) -> luma:uid().
get_display_uid(#luma_space{display_uid = DisplayUid}) ->
    DisplayUid.

-spec get_display_gid(entry()) -> luma:gid().
get_display_gid(#luma_space{display_gid = DisplayGid}) ->
    DisplayGid.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function ensures that all necessary fields are set.
%% If IgnoreLumaDefault == true default_uid and default_gid can be undefined.
%% Otherwise all fields must be set.
%% @end
%%--------------------------------------------------------------------
-spec ensure_all_fields_defined(entry(), od_space:id(), storage:data(), IsPosix :: boolean()) -> entry().
ensure_all_fields_defined(LumaSpace, SpaceId, Storage, true) ->
    ensure_all_fields_defined(LumaSpace, SpaceId, Storage);
ensure_all_fields_defined(LumaSpace, SpaceId, Storage, false) ->
    ensure_display_fields_defined(LumaSpace, SpaceId, Storage).


-spec ensure_all_fields_defined(entry(), od_space:id(), storage:data()) -> entry().
ensure_all_fields_defined(LS = #luma_space{
    default_uid = DefaultUid,
    default_gid = DefaultGid,
    display_uid = DisplayUid,
    display_gid = DisplayGid
}, _SpaceId, _Storage)
    when DefaultUid =/= undefined
    andalso DefaultGid =/= undefined
->
    % both default_ fields are defined
    % use default_ fields as display_ fallbacks
    LS#luma_space{
        display_uid = ensure_defined(DisplayUid, DefaultUid),
        display_gid = ensure_defined(DisplayGid, DefaultGid)
    };
ensure_all_fields_defined(LS = #luma_space{
    default_uid = DefaultUid,
    default_gid = DefaultGid,
    display_uid = DisplayUid,
    display_gid = DisplayGid
}, SpaceId, Storage) ->
    % at least one of default_ fields is undefined
    {FallbackUid, FallbackGid} = get_posix_compatible_fallback_credentials(Storage, SpaceId),
    DefaultUid2 = ensure_defined(DefaultUid, FallbackUid),
    DefaultGid2 = ensure_defined(DefaultGid, FallbackGid),
    LS#luma_space{
        default_uid = DefaultUid2,
        default_gid = DefaultGid2,
        display_uid = ensure_defined(DisplayUid, DefaultUid2),
        display_gid = ensure_defined(DisplayGid, DefaultGid2)
    }.

-spec ensure_display_fields_defined(entry(), od_space:id(), storage:id()) -> entry().
ensure_display_fields_defined(LS = #luma_space{
    display_uid = DisplayUid,
    display_gid = DisplayGid
}, _SpaceId, _Storage)
    when DisplayUid =/= undefined
    andalso DisplayGid =/= undefined
->
    % default_ fields can be undefined
    LS;
ensure_display_fields_defined(LS = #luma_space{
    display_uid = DisplayUid,
    display_gid = DisplayGid
}, SpaceId, Storage) ->
    % default_ fields can be undefined
    % at least one of display_ fields is undefined
    {FallbackUid, FallbackGid} = get_posix_compatible_fallback_credentials(Storage, SpaceId),
    LS#luma_space{
        display_uid = ensure_defined(DisplayUid, FallbackUid),
        display_gid = ensure_defined(DisplayGid, FallbackGid)
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function returns fallback posix credentials for passed
%% Storage and Space.
%% If storage is POSIX compatible, it returns UID and GID of storage
%% mountpoint directory.
%% Otherwise, it generates UID and GID.
%% UID is generated using id of virtual SpaceOwner as hashing base.
%% GID is generated using SpaceId as hashing base
%% @end
%%--------------------------------------------------------------------
-spec get_posix_compatible_fallback_credentials(storage:data(), od_space:id()) ->
    {luma:uid(), luma:gid()}.
get_posix_compatible_fallback_credentials(Storage, SpaceId) ->
    case storage:is_posix_compatible(Storage) of
        true ->
            get_mountpoint_credentials(Storage, SpaceId);
        false ->
            Uid = luma_utils:generate_uid(?SPACE_OWNER_ID(SpaceId)),
            Gid = luma_utils:generate_gid(SpaceId),
            {Uid, Gid}
    end.


-spec get_mountpoint_credentials(storage:data(), od_space:id()) -> {luma:uid(), luma:gid()}.
get_mountpoint_credentials(Storage, SpaceId) ->
    StorageFileCtx = storage_file_ctx:new(?DIRECTORY_SEPARATOR_BINARY, SpaceId, storage:get_id(Storage)),
    {#statbuf{st_uid = Uid, st_gid = Gid}, _} = storage_file_ctx:stat(StorageFileCtx),
    {Uid, Gid}.


-spec ensure_defined(term(), term()) -> term().
ensure_defined(Value, Default) ->
    utils:ensure_defined(Value, undefined, Default).