%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2017 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This module implements generic, temporary cache behaviour for luma
%%% and reverse_luma models. Entries are valid only for specific period.
%%% After that period, callback is called to acquire current value.
%%% @end
%%%-------------------------------------------------------------------
-module(luma_cache).
-author("Jakub Kudzia").

-include("global_definitions.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([invalidate/1, get_user_ctx/4, get_user_id/3, get_group_id/3, get_user_ctx/5]).

%% datastore_model callbacks
-export([get_ctx/0, get_record_struct/1]).

-type tree_root_id_prefix() :: binary(). % ?LUMA_USER_ROOT | ?REV_LUMA_USER_ROOT | REV_LUMA_GROUP_ROOT
-type tree_root_id() :: binary().   % tree_root_id_prefix() concatenated with storage:id()
-type link_id() :: binary().
-type link_target() :: od_user:id() | od_group:id()| luma:user_ctx().
-type query_fun() :: fun(() -> {ok, luma:user_ctx() |od_user:od() | od_group:id()}
                                | {error, term()}).

-define(CTX, #{
    model => ?MODULE,
    routing => global
}).

-define(LUMA_USER_ROOT, <<"luma_user_">>).
-define(REV_LUMA_USER_ROOT, <<"rev_luma_user_">>).
-define(REV_LUMA_GROUP_ROOT, <<"rev_luma_group_">>).
-define(ROOTS, [?LUMA_USER_ROOT, ?REV_LUMA_USER_ROOT, ?REV_LUMA_GROUP_ROOT]).

-define(SEP, <<"##">>).

%%%===================================================================
%%% API functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec get_user_ctx(od_user:id(), storage:id(), query_fun(), helper:name()) ->
    {ok, luma:user_ctx()} | {error, term()}.
get_user_ctx(UserId, StorageId, QueryFun, HelperName) ->
    case get_links(?LUMA_USER_ROOT, StorageId, UserId) of
        {ok, EncodedUserCtx} ->
            {ok, decode_user_ctx(EncodedUserCtx, HelperName)};
        {error, not_found} ->
            try QueryFun() of
                {ok, UserCtx} ->
                    EncodedUserCtx = encode_user_ctx(UserCtx, HelperName),
                    add_link(?LUMA_USER_ROOT, StorageId, UserId, EncodedUserCtx),
                    maybe_add_reverse_mapping(StorageId, UserCtx, UserId),
                    {ok, UserCtx};
                Error ->
                    ?error_stacktrace("Fetching user_ctx from LUMA failed due to ~p", [Error]),
                    Error
            catch
                Error:Reason ->
                    ?error_stacktrace("Fetching user_ctx from LUMA failed due to ~p", [{Error, Reason}]),
                    {error, Reason}
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec get_user_ctx(od_user:id(), storage:id(), od_group:id() | od_space:id(),
    query_fun(), helper:name()) -> {ok, luma:user_ctx()} | {error, term()}.
get_user_ctx(UserId, StorageId, GroupOrSpaceId, QueryFun, HelperName) ->
    case get_links(?LUMA_USER_ROOT, StorageId, UserId) of
        {ok, EncodedUserCtx} ->
            {ok, decode_user_ctx(EncodedUserCtx, HelperName)};
        {error, not_found} ->
            try QueryFun() of
                {ok, UserCtx} ->
                    EncodedUserCtx = encode_user_ctx(UserCtx, HelperName),
                    add_link(?LUMA_USER_ROOT, StorageId, UserId, EncodedUserCtx),
                    maybe_add_reverse_mapping(StorageId, UserCtx, UserId, GroupOrSpaceId),
                    {ok, UserCtx};
                Error ->
                    ?error_stacktrace("Fetching user_ctx from LUMA failed due to ~p", [Error]),
                    Error
            catch
                Error:Reason ->
                    ?error_stacktrace("Fetching user_ctx from LUMA failed due to ~p", [{Error, Reason}]),
                    {error, Reason}
            end
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec get_user_id(non_neg_integer() | binary(), storage:id(), query_fun()) ->
    {ok, od_user:id()} | {error, term()}.
get_user_id(UidOrName, StorageId, QueryFun) ->
    UidOrNameBin = str_utils:to_binary(UidOrName),
    case get_links(?REV_LUMA_USER_ROOT, StorageId, UidOrNameBin) of
        {ok, UserId} ->
            {ok, UserId};
        {error, not_found} ->
            try QueryFun() of
                {ok, UserId} ->
                    add_link(?REV_LUMA_USER_ROOT, StorageId, UidOrNameBin, UserId),
                    {ok, UserId};
                Error ->
                    ?error_stacktrace("Fetching user_id from LUMA failed due to ~p", [Error]),
                    Error
            catch
                Error:Reason ->
                    ?error_stacktrace("Fetching user_id from LUMA failed due to ~p", [{Error, Reason}]),
                    {error, Reason}
            end
    end.

-spec get_group_id(non_neg_integer() | binary(), storage:id(), query_fun()) ->
    {ok, od_group:id()} | {error, term()}.
get_group_id(GidOrName, StorageId, QueryFun) ->
    GidOrNameBin = str_utils:to_binary(GidOrName),
    case get_links(?REV_LUMA_GROUP_ROOT, StorageId, GidOrNameBin) of
        {ok, GroupId} ->
            {ok, GroupId};
        {error, not_found} ->
            try QueryFun() of
                {ok, undefined} ->
                    ?warning("Fetching get_group_id from LUMA returned undefined"),
                    {ok, undefined};
                {ok, GroupId} when is_binary(GroupId) ->
                    add_link(?REV_LUMA_GROUP_ROOT, StorageId, GidOrNameBin, GroupId),
                    {ok, GroupId};
                Error ->
                    ?error_stacktrace("Fetching get_group_id from LUMA failed due to ~p", [Error]),
                    Error
            catch
                Error:Reason ->
                    ?error_stacktrace("Fetching get_group_id from LUMA failed due to ~p", [{Error, Reason}]),
                    {error, Reason}
            end
    end.


%%-------------------------------------------------------------------
%% @doc
%% Deletes all cached entries.
%% @end
%%-------------------------------------------------------------------
-spec invalidate(storage:id()) -> ok.
invalidate(StorageId) ->
    lists:foreach(fun(RootId) ->
        {ok, LinkIds} = for_each(RootId, StorageId, fun(LinkId, Acc) ->
            [LinkId |Acc]
        end, []),
        lists:foreach(fun(LinkId) ->
            ok = delete_link(RootId, StorageId, LinkId)
        end, LinkIds)
    end, ?ROOTS).


%%%===================================================================
%%% Internal functions
%%%===================================================================

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec maybe_add_reverse_mapping(od_storage:id(), luma:user_ctx(), od_user:id()) -> ok.
maybe_add_reverse_mapping(StorageId, #{<<"uid">> := Uid}, UserId) ->
    add_link(?REV_LUMA_USER_ROOT, StorageId, Uid, UserId);
maybe_add_reverse_mapping(_StorageId, _UserCtx, _UserId) ->
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec maybe_add_reverse_mapping(od_storage:id(), luma:user_ctx(), od_user:id(),
    od_group:id() | od_space:id()) -> ok.
maybe_add_reverse_mapping(StorageId, #{<<"uid">> := Uid, <<"gid">> := Gid}, UserId, GroupOrSpaceId) ->
    add_link(?REV_LUMA_USER_ROOT, StorageId, Uid, UserId),
    add_link(?REV_LUMA_GROUP_ROOT, StorageId, Gid, GroupOrSpaceId);
maybe_add_reverse_mapping(_StorageId, _UserCtx, _UserId, _GroupOrSpaceId) ->
    ok.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec decode_user_ctx(binary(), helper:name()) -> luma:user_ctx().
decode_user_ctx(Encoded, ?CEPH_HELPER_NAME) ->
    [UserName, Key] = binary:split(Encoded, ?SEP, [global]),
    #{<<"username">> => UserName, <<"key">> => Key};
decode_user_ctx(Encoded, ?S3_HELPER_NAME) ->
    [AccessKey, SecretKey] = binary:split(Encoded, ?SEP, [global]),
    #{<<"accessKey">> => AccessKey, <<"secretKey">> => SecretKey};
decode_user_ctx(Encoded, ?SWIFT_HELPER_NAME) ->
    [UserName, Password] = binary:split(Encoded, ?SEP, [global]),
    #{<<"username">> => UserName, <<"password">> => Password};
decode_user_ctx(Encoded, HelperName) when
    HelperName =:= ?POSIX_HELPER_NAME orelse
        HelperName =:= ?GLUSTERFS_HELPER_NAME orelse
        HelperName =:= ?NULL_DEVICE_HELPER_NAME
    ->
    [Uid, Gid] = binary:split(Encoded, ?SEP, [global]),
    #{<<"uid">> => Uid, <<"gid">> => Gid}.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec encode_user_ctx(luma:user_ctx(), helper:name()) -> binary().
encode_user_ctx(#{<<"username">> := UserName, <<"key">> := Key}, ?CEPH_HELPER_NAME) ->
    encode(UserName, Key);
encode_user_ctx(#{<<"accessKey">> := AccessKey, <<"secretKey">> := SecretKey}, ?S3_HELPER_NAME) ->
    encode(AccessKey, SecretKey);
encode_user_ctx(#{<<"username">> := UserName, <<"password">> := Password}, ?SWIFT_HELPER_NAME) ->
    encode(UserName, Password);
encode_user_ctx(#{<<"uid">> := Uid, <<"gid">> := Gid}, HelperName) when
    HelperName =:= ?POSIX_HELPER_NAME orelse
        HelperName =:= ?GLUSTERFS_HELPER_NAME orelse
        HelperName =:= ?NULL_DEVICE_HELPER_NAME
    ->
    encode(Uid, Gid).

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec encode(binary(), binary()) -> binary().
encode(Value1, Value2) ->
    <<Value1/binary, ?SEP/binary, Value2/binary>>.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% Fetches link.
%% @end
%%-------------------------------------------------------------------
-spec get_links(tree_root_id_prefix(), storage:id(), link_id()) ->
    {ok, link_target()} | {error, term()}.
get_links(RootId, StorageId, LinkId) ->
    TreeRoot = tree_root(RootId, StorageId),
    case datastore_model:get_links(?CTX, TreeRoot, all, LinkId) of
        {ok, [#link{target = Target}]} ->
            {ok, Target};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%--------------------------------------------------------------------
-spec add_link(tree_root_id_prefix(), storage:id(), link_id(), link_target()) -> ok.
add_link(RootId, StorageId, LinkId, LinkTarget) ->
    TreeId = oneprovider:get_id(),
    TreeRoot = tree_root(RootId, StorageId),
    case datastore_model:add_links(?CTX, TreeRoot, TreeId, {LinkId, LinkTarget}) of
        {ok, _} -> ok;
        {error, already_exists} -> ok
    end.

%%-------------------------------------------------------------------
%% @private
%% @doc
%% WRITEME
%% @end
%%-------------------------------------------------------------------
-spec delete_link(tree_root_id_prefix(), storage:id(), link_id()) -> ok.
delete_link(RootId, StorageId, LinkId) ->
    TreeId = oneprovider:get_id(),
    TreeRoot = tree_root(RootId, StorageId),
    case datastore_model:delete_links(?CTX, TreeRoot, TreeId, LinkId) of
        [] ->
            ok;
        ok ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Executes callback for each link.
%% @end
%%--------------------------------------------------------------------
-spec for_each(
    tree_root_id_prefix(), storage:id(),
    Callback :: fun((link_id(), Acc0 :: term()) -> Acc :: term()),
    Acc0 :: term()) -> {ok, Acc :: term()} | {error, term()}.
for_each(RootId, StorageId, Callback, Acc0) ->
    TreeRoot = tree_root(RootId, StorageId),
    datastore_model:fold_links(?CTX, TreeRoot, all, fun
        (#link{name = Name}, Acc) ->
            {ok, Callback(Name, Acc)}
    end, Acc0, #{}).


%%-------------------------------------------------------------------
%% @private
%% @doc
%% Returns link's tree root id based on given RootPrefix and StorageId.
%% @end
%%-------------------------------------------------------------------
-spec tree_root(binary(), storage:id()) -> tree_root_id().
tree_root(RootPrefix, StorageId) ->
    <<RootPrefix/binary, StorageId/binary>>.


%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's context.
%% @end
%%--------------------------------------------------------------------
-spec get_ctx() -> datastore:ctx().
get_ctx() ->
    ?CTX.

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, []}.