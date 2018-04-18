%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This module is responsible for handing requests operating on directories.
%%% @end
%%%--------------------------------------------------------------------
-module(dir_req).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("proto/oneclient/fuse_messages.hrl").
-include_lib("ctool/include/posix/acl.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_links.hrl").

%% API
-export([mkdir/4, read_dir/5, read_dir_plus/5]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv mkdir_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec mkdir(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    fslogic_worker:fuse_response().
mkdir(UserCtx, ParentFileCtx, Name, Mode) ->
    check_permissions:execute(
        [traverse_ancestors, ?traverse_container, ?add_subcontainer],
        [UserCtx, ParentFileCtx, Name, Mode],
        fun mkdir_insecure/4).

%%--------------------------------------------------------------------
%% @equiv read_dir_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec read_dir(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: non_neg_integer(), Limit :: non_neg_integer(),
    Token :: undefined | binary()) -> fslogic_worker:fuse_response().
read_dir(UserCtx, FileCtx, Offset, Limit, Token) ->
    check_permissions:execute(
        [traverse_ancestors, ?list_container],
        [UserCtx, FileCtx, Offset, Limit, Token],
        fun read_dir_insecure/5).

%%--------------------------------------------------------------------
%% @equiv read_dir_plus_insecure/5 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec read_dir_plus(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: non_neg_integer(), Limit :: non_neg_integer(),
    Token :: undefined | binary()) -> fslogic_worker:fuse_response().
read_dir_plus(UserCtx, FileCtx, Offset, Limit, Token) ->
    check_permissions:execute(
        [traverse_ancestors, ?traverse_container, ?list_container],
        [UserCtx, FileCtx, Offset, Limit, Token],
        fun read_dir_plus_insecure/5).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir_insecure(user_ctx:ctx(), ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(), Mode :: file_meta:posix_permissions()) ->
    fslogic_worker:fuse_response().
mkdir_insecure(UserCtx, ParentFileCtx, Name, Mode) ->
    CTime = time_utils:cluster_time_seconds(),
    File = #document{value = #file_meta{
        name = Name,
        type = ?DIRECTORY_TYPE,
        mode = Mode,
        owner = user_ctx:get_user_id(UserCtx)
    }},
    ParentUuid = file_ctx:get_uuid_const(ParentFileCtx),
    {ok, DirUuid} = file_meta:create({uuid, ParentUuid}, File), %todo maybe pass file_ctx inside
    SpaceId = file_ctx:get_space_id_const(ParentFileCtx),
    {ok, _} = times:save(#document{
        key = DirUuid,
        value = #times{mtime = CTime, atime = CTime, ctime = CTime},
        scope = SpaceId
    }),
    fslogic_times:update_mtime_ctime(ParentFileCtx),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #dir{guid = fslogic_uuid:uuid_to_guid(DirUuid, SpaceId)}
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists directory. Starts with Offset entity and limits returned list to Limit size.
%% @end
%%--------------------------------------------------------------------
-spec read_dir_insecure(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: non_neg_integer(), Limit :: non_neg_integer(),
    Token :: undefined | binary()) -> fslogic_worker:fuse_response().
read_dir_insecure(UserCtx, FileCtx, Offset, Limit, Token) ->
    {Token2, CachePid} = get_cached_token(Token),

    {Children, NewToken, IsLast, FileCtx2} = case
        file_ctx:get_file_children(FileCtx, UserCtx, Offset, Limit, Token2) of
        {C, FC}  -> {C, <<"">>, length(C) < Limit, FC};
        {C, NT, FC} ->
            IL = NT#link_token.is_last,
            NT2 = case IL of
                true -> <<"">>;
                _ -> cache_token(NT, CachePid)
            end,
            {C, NT2, IL, FC}
    end,

    ChildrenLinks =
        lists:map(fun(ChildFile) ->
            ChildGuid = file_ctx:get_guid_const(ChildFile),
            {ChildName, _ChildFile3} = file_ctx:get_aliased_name(ChildFile, UserCtx),
            #child_link{name = ChildName, guid = ChildGuid}
        end, Children),
    fslogic_times:update_atime(FileCtx2),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children{
            child_links = ChildrenLinks,
            index_token = NewToken,
            is_last = IsLast
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists directory with stats of each file.
%% Starts with Offset entity and limits returned list to Limit size.
%% @end
%%--------------------------------------------------------------------
-spec read_dir_plus_insecure(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: non_neg_integer(), Limit :: non_neg_integer(),
    Token :: undefined | binary()) -> fslogic_worker:fuse_response().
read_dir_plus_insecure(UserCtx, FileCtx, Offset, Limit, Token) ->
    {Token2, CachePid} = get_cached_token(Token),

    {Children, NewToken, IsLast, FileCtx2} = case
        file_ctx:get_file_children(FileCtx, UserCtx, Offset, Limit, Token2) of
        {C, FC}  -> {C, <<"">>, length(C) < Limit, FC};
        {C, NT, FC} ->
            IL = NT#link_token.is_last,
            NT2 = case IL of
                true -> <<"">>;
                _ -> cache_token(NT, CachePid)
            end,
            {C, NT2, IL, FC}
    end,

    MapFun = fun(ChildCtx) ->
        try
            #fuse_response{
                status = #status{code = ?OK},
                fuse_response = Attrs
            } = attr_req:get_file_attr_insecure(UserCtx, ChildCtx),
            Attrs
        catch
            _:_ ->
                % File can be not synchronized with other provider
                error
        end
    end,
    FilterFun = fun
        (error) -> false;
        (_Attrs) -> true
    end,
    MaxProcs = application:get_env(?APP_NAME, max_read_dir_plus_procs, 200),
    ChildrenAttrs = filtermap(MapFun, FilterFun, Children, MaxProcs, length(Children)),

    fslogic_times:update_atime(FileCtx2),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children_attrs{
            child_attrs = ChildrenAttrs,
            index_token = NewToken,
            is_last = IsLast
        }
    }.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets token value from cache or via decoding.
%% @end
%%--------------------------------------------------------------------
-spec get_cached_token(Token :: undefined | binary()) ->
    {CachedToken :: datastore_links_iter:token(), CachePid :: pid() | undefined}.
get_cached_token(<<"">>) ->
    {#link_token{}, undefined};
get_cached_token(undefined) ->
    {#link_token{}, undefined};
get_cached_token(Token) ->
    case application:get_env(?APP_NAME, cache_list_dir_token, false) of
        true ->
            CachePid = binary_to_term(Token),
            CachePid ! {get_token, self()},
            receive
                {link_token, CachedToken} -> {CachedToken, CachePid}
            after
                1000 -> {#link_token{}, undefined}
            end;
        _ ->
            {binary_to_term(Token), undefined}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Encodes and caches token if needed.
%% @end
%%--------------------------------------------------------------------
-spec cache_token(Token :: datastore_links_iter:token(),
    CachePid :: pid() | undefined) -> binary().
cache_token(NT, undefined) ->
    case application:get_env(?APP_NAME, cache_list_dir_token, false) of
        true ->
            Pid = spawn(fun() ->
                cache_proc(NT)
            end),
            term_to_binary(Pid);
        _ ->
            term_to_binary(NT)
    end;
cache_token(NT, CachePid) ->
    case rpc:call(node(CachePid), erlang, is_process_alive, [CachePid]) of
        true ->
            CachePid ! {cache_token, NT},
            term_to_binary(CachePid);
        _ ->
            cache_token(NT, undefined)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Token cache process.
%% @end
%%--------------------------------------------------------------------
-spec cache_proc(Token :: datastore_links_iter:token()) -> ok.
cache_proc(Token) ->
    Timeout = application:get_env(?APP_NAME,
        list_dir_token_cache_timeout, timer:seconds(30)),
    receive
        {get_token, Pid} ->
            Pid ! {link_token, Token},
            cache_proc(Token);
        {cache_token, NT} ->
            cache_proc(NT)
    after
        Timeout ->
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% A parallel function similar to lists:filtermap/2. See {@link lists:filtermap/2}.
%% However, Filter and Map functions are separeted and number of parallel
%% processes is limited.
%% @end
%%--------------------------------------------------------------------
-spec filtermap(Map :: fun((X :: A) -> B), Filter :: fun((X :: A) -> boolean()),
    L :: [A], MaxProcs :: non_neg_integer(), Length :: non_neg_integer()) -> [B].
filtermap(Map, Filter, L, MaxProcs, Length) ->
    case Length > MaxProcs of
        true ->
            {L1, L2} = lists:split(MaxProcs, L),
            filtermap(Map, Filter, L1) ++
                filtermap(Map, Filter, L2, MaxProcs, Length - MaxProcs);
        _ ->
            filtermap(Map, Filter, L)
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% A parallel function similar to lists:filtermap/2. See {@link lists:filtermap/2}.
%% However, Filter and Map functions are separeted.
%% @end
%%--------------------------------------------------------------------
-spec filtermap(Map :: fun((X :: A) -> B), Filter :: fun((X :: A) -> boolean()),
    L :: [A]) -> [B].
filtermap(Map, Filter, L) ->
    LWithNum = lists:zip(lists:seq(1, length(L)), L),
    Mapped = utils:pmap(fun({Num, Element}) ->
        {Num, Map(Element)}
    end, LWithNum),

    lists:filtermap(fun
        ({_, error}) -> false;
        ({_, Ans}) ->
            case Filter(Ans) of
                true ->
                    {true, Ans};
                _ ->
                    false
            end
    end, lists:sort(Mapped)).