%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016-2020 ACK CYFRONET AGH
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
-export([
    mkdir/4,
    get_children/5, get_children/6,
    get_children_attrs/5,
    get_children_info/5
]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @equiv mkdir_insecure/4 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec mkdir(
    user_ctx:ctx(),
    ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()
) ->
    fslogic_worker:fuse_response().
mkdir(UserCtx, ParentFileCtx0, Name, Mode) ->
    ParentFileCtx1 = fslogic_authz:ensure_authorized(
        UserCtx, ParentFileCtx0,
        [traverse_ancestors, ?traverse_container, ?add_subcontainer]
    ),
    mkdir_insecure(UserCtx, ParentFileCtx1, Name, Mode).


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_children(UserCtx, FileCtx, Offset, Limit, Token, undefined).
%% @end
%%--------------------------------------------------------------------
-spec get_children(
    user_ctx:ctx(),
    file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | binary()
) ->
    fslogic_worker:fuse_response().
get_children(UserCtx, FileCtx, Offset, Limit, Token) ->
    get_children(UserCtx, FileCtx, Offset, Limit, Token, undefined).


%%--------------------------------------------------------------------
%% @doc
%% @equiv get_children_insecure/7 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_children(
    user_ctx:ctx(),
    file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | binary(),
    StartId :: undefined | file_meta:name()
) ->
    fslogic_worker:fuse_response().
get_children(UserCtx, FileCtx0, Offset, Limit, Token, StartId) ->
    {ChildrenWhiteList, FileCtx1} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?list_container]
    ),
    get_children_insecure(
        UserCtx, FileCtx1, Offset, Limit, Token, StartId, ChildrenWhiteList
    ).


%%--------------------------------------------------------------------
%% @equiv get_children_attrs_insecure/6 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_children_attrs(
    user_ctx:ctx(),
    file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | binary()
) ->
    fslogic_worker:fuse_response().
get_children_attrs(UserCtx, FileCtx0, Offset, Limit, Token) ->
    {ChildrenWhiteList, FileCtx1} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?traverse_container, ?list_container]
    ),
    get_children_attrs_insecure(
        UserCtx, FileCtx1, Offset, Limit, Token, ChildrenWhiteList
    ).


%%--------------------------------------------------------------------
%% @equiv get_children_info_insecure/6 with permission checks
%% @end
%%--------------------------------------------------------------------
-spec get_children_info(
    user_ctx:ctx(),
    file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    StartId :: undefined | file_meta:name()
) ->
    fslogic_worker:fuse_response().
get_children_info(UserCtx, FileCtx0, Offset, Limit, StartId) ->
    {ChildrenWhiteList, FileCtx1} = fslogic_authz:ensure_authorized_readdir(
        UserCtx, FileCtx0,
        [traverse_ancestors, ?traverse_container, ?list_container]
    ),
    get_children_info_insecure(
        UserCtx, FileCtx1, Offset, Limit, StartId, ChildrenWhiteList
    ).


%%%===================================================================
%%% Internal functions
%%%===================================================================


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Creates new directory.
%% @end
%%--------------------------------------------------------------------
-spec mkdir_insecure(
    user_ctx:ctx(),
    ParentFileCtx :: file_ctx:ctx(),
    Name :: file_meta:name(),
    Mode :: file_meta:posix_permissions()
) ->
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

    FileCtx = file_ctx:new_by_guid(file_id:pack_guid(DirUuid, SpaceId)),
    #fuse_response{fuse_response = FileAttr} =
        attr_req:get_file_attr_light(UserCtx, FileCtx, false),
    FileAttr2 = FileAttr#file_attr{size = 0},
    ok = fslogic_event_emitter:emit_file_attr_changed(FileCtx, FileAttr2, [user_ctx:get_session_id(UserCtx)]),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #dir{guid = file_id:pack_guid(DirUuid, SpaceId)}
    }.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Lists directory. Starts with Offset entity and limits returned list to Limit size.
%% @end
%%--------------------------------------------------------------------
-spec get_children_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | binary(),
    StartId :: undefined | file_meta:name(),
    ChildrenWhiteList :: undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_insecure(UserCtx, FileCtx0, Offset, Limit, Token, StartId, ChildrenWhiteList) ->
    {Children, NewToken, IsLast, FileCtx1} = list_children(
        UserCtx, FileCtx0, Offset, Limit, Token, StartId, ChildrenWhiteList
    ),
    ChildrenLinks = lists:map(fun(ChildFile) ->
        ChildGuid = file_ctx:get_guid_const(ChildFile),
        {ChildName, _ChildFile3} = file_ctx:get_aliased_name(ChildFile, UserCtx),
        #child_link{name = ChildName, guid = ChildGuid}
    end, Children),

    fslogic_times:update_atime(FileCtx1),
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
-spec get_children_attrs_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | binary(),
    ChildrenWhiteList :: undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_attrs_insecure(UserCtx, FileCtx0, Offset, Limit, Token, ChildrenWhiteList) ->
    {Children, NewToken, IsLast, FileCtx1} = list_children(
        UserCtx, FileCtx0, Offset, Limit, Token, undefined, ChildrenWhiteList
    ),
    MapFun = fun(ChildCtx) ->
        #fuse_response{
            status = #status{code = ?OK},
            fuse_response = Attrs
        } = attr_req:get_file_attr_light(UserCtx, ChildCtx, true),
        Attrs
    end,
    ChildrenAttrs = process_children(MapFun, Children),

    fslogic_times:update_atime(FileCtx1),
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
%% Lists directory with info of each file.
%% Starts with Offset entity and limits returned list to Limit size.
%% @end
%%--------------------------------------------------------------------
-spec get_children_info_insecure(
    user_ctx:ctx(),
    file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    StartId :: undefined | file_meta:name(),
    ChildrenWhiteList :: undefined | [file_meta:name()]
) ->
    fslogic_worker:fuse_response().
get_children_info_insecure(UserCtx, FileCtx0, Offset, Limit, StartId, ChildrenWhiteList) ->
    {Children, _NewToken, IsLast, FileCtx1} = list_children(
        UserCtx, FileCtx0, Offset, Limit, undefined, StartId, ChildrenWhiteList
    ),
    MapFun = fun(ChildCtx) ->
        #fuse_response{
            status = #status{code = ?OK},
            fuse_response = FileInfo
        } = attr_req:get_file_info(UserCtx, ChildCtx),
        FileInfo
    end,
    ChildrenInfo = process_children(MapFun, Children),

    fslogic_times:update_atime(FileCtx1),
    #fuse_response{status = #status{code = ?OK},
        fuse_response = #file_children_info{
            child_info = ChildrenInfo,
            is_last = IsLast
        }
    }.


%% @private
-spec list_children(user_ctx:ctx(), file_ctx:ctx(),
    Offset :: file_meta:offset(),
    Limit :: file_meta:limit(),
    Token :: undefined | datastore_links_iter:token(),
    StartId :: undefined | file_meta:name(),
    ChildrenWhiteList :: undefined | [file_meta:name()]
) ->
    {
        Children :: [file_ctx:ctx()],
        NewToken :: binary(),
        IsLast :: boolean(),
        NewFileCtx :: file_ctx:ctx()
    }.
list_children(UserCtx, FileCtx, Offset, Limit, Token, StartId, undefined) ->
    {Token2, CachePid} = get_cached_token(Token),

    case file_ctx:get_file_children(FileCtx, UserCtx, Offset, Limit, Token2, StartId) of
        {C, FC}  ->
            {C, <<"">>, length(C) < Limit, FC};
        {C, NT, FC} ->
            IL = NT#link_token.is_last,
            NT2 = case IL of
                true -> <<"">>;
                _ -> cache_token(NT, CachePid)
            end,
            {C, NT2, IL, FC}
    end;
list_children(UserCtx, FileCtx0, Offset, Limit, _, StartId, ChildrenWhiteList0) ->
    ChildrenWhiteList1 = case StartId of
        undefined ->
            ChildrenWhiteList0;
        _ ->
            lists:dropwhile(fun(Name) -> Name < StartId end, ChildrenWhiteList0)
    end,
    {Children, FileCtx1} = file_ctx:get_file_children_whitelisted(
        FileCtx0, UserCtx, Offset, Limit, ChildrenWhiteList1
    ),
    {Children, <<>>, length(Children) < Limit, FileCtx1}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Calls MapFunctionInsecure for every passed children in parallel and
%% filters out children for which it raised error.
%% @end
%%--------------------------------------------------------------------
-spec process_children(
    MapFunInsecure :: fun((ChildCtx :: file_ctx:ctx()) -> Result),
    Children :: [file_ctx:ctx()]
) ->
    [Result] when Result :: term().
process_children(MapFunInsecure, Children) ->
    MapFun = fun(ChildCtx) ->
        try
            MapFunInsecure(ChildCtx)
        catch _:_ ->
            % File can be not synchronized with other provider
            error
        end
    end,
    FilterFun = fun
        (error) -> false;
        (_Attrs) -> true
    end,
    MaxProcesses = application:get_env(?APP_NAME, max_read_dir_plus_procs, 20),
    filtermap(MapFun, FilterFun, Children, MaxProcesses, length(Children)).


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
