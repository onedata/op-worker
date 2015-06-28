%%%-------------------------------------------------------------------
%%% @author Rafal Slota
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc @todo: Write me!
%%% @end
%%%-------------------------------------------------------------------
-module(cberl_internal).
-author("Rafal Slota").
-include_lib("ctool/include/logging.hrl").

%% API
-export([]).

%%%===================================================================
%%% API
%%%===================================================================

-define('CBE_ADD',      1).
-define('CBE_REPLACE',  2).
-define('CBE_SET',      3).
-define('CBE_APPEND',   4).
-define('CBE_PREPEND',  5).

-define('CMD_CONNECT',    0).
-define('CMD_STORE',      1).
-define('CMD_MGET',       2).
-define('CMD_UNLOCK',     3).
-define('CMD_MTOUCH',     4).
-define('CMD_ARITHMETIC', 5).
-define('CMD_REMOVE',     6).
-define('CMD_HTTP',       7).

-type handle() :: binary().

-record(instance, {handle :: handle(),
    bucketname :: string(),
    transcoder :: module(),
    connected :: true | false,
    opts :: list()}).

-type key() :: string().
-type value() :: string() | list() | integer() | binary().
-type operation_type() :: add | replace | set | append | prepend.
-type instance() :: #instance{}.
-type http_type() :: view | management | raw.
-type http_method() :: get | post | put | delete.

%% -export([start_link/2, start_link/3, start_link/5, start_link/6, start_link/7]).
%% -export([stop/1]).
%% store operations
-export([add/4, add/5, replace/4, replace/5, set/4, set/5, store/7]).
%% update operations
-export([append/3, prepend/3, touch/3, mtouch/3]).
-export([incr/3, incr/4, incr/5, decr/3, decr/4, decr/5]).
-export([arithmetic/6]).
-export([append/4, prepend/4]).
%% retrieval operations
-export([get_and_touch/3, get_and_lock/3, mget/2, get/2, unlock/3,
    mget/3, getl/3, http/6, view/4, foldl/3, foldr/3, foreach/2]).
%% removal operations
-export([remove/2, flush/1, flush/2]).
%% design doc opertations
-export([set_design_doc/3, remove_design_doc/2]).
-deprecated({append, 4}).
-deprecated({prepend, 4}).

%%%%%%%%%%%%%%%%%%%%%%%%
%%% STORE OPERATIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%

%% @equiv add(PoolPid, Key, Exp, Value, standard)
-spec add(pid(), key(), integer(), value()) -> ok | {error, _}.
add(PoolPid, Key, Exp, Value) ->
    add(PoolPid, Key, Exp, Value, standard).

%% @equiv store(PoolPid, add, Key, Value, TranscoderOpts, Exp, 0)
-spec add(pid(), key(), integer(), value(), atom()) -> ok | {error, _}.
add(PoolPid, Key, Exp, Value, TranscoderOpts) ->
    store(PoolPid, add, Key, Value, TranscoderOpts, Exp, 0).

%% @equiv replace(PoolPid, Key, Exp, Value, standard)
-spec replace(pid(), key(), integer(), value()) -> ok | {error, _}.
replace(PoolPid, Key, Exp, Value) ->
    replace(PoolPid, Key, Exp, Value, standard).

%% @equiv store(PoolPid, replace, "", Key, Value, Exp)
-spec replace(pid(), key(), integer(), value(), atom()) -> ok | {error, _}.
replace(PoolPid, Key, Exp, Value, TranscoderOpts) ->
    store(PoolPid, replace, Key, Value, TranscoderOpts, Exp, 0).

%% @equiv set(PoolPid, Key, Exp, Value, standard)
-spec set(pid(), key(), integer(), value()) -> ok | {error, _}.
set(PoolPid, Key, Exp, Value) ->
    set(PoolPid, Key, Exp, Value, standard).

%% @equiv store(PoolPid, set, "", Key, Value, Exp)
-spec set(pid(), key(), integer(), value(), atom()) -> ok | {error, _}.
set(PoolPid, Key, Exp, Value, TranscoderOpts) ->
    store(PoolPid, set, Key, Value, TranscoderOpts, Exp, 0).

%%%%%%%%%%%%%%%%%%%%%%%%%
%%% UPDATE OPERATIONS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%

%% @deprecated
%% @equiv append(PoolPid, Key, Value)
%% @doc Deprecated append function which accepts an _unused_ CAS value
-spec append(pid(), integer(), key(), value()) -> ok | {error, _}.
append(PoolPid, _Cas, Key, Value) ->
    append(PoolPid, Key, Value).

-spec append(pid(), key(), value()) -> ok | {error, _}.
append(PoolPid, Key, Value) ->
    store(PoolPid, append, Key, Value, none, 0, 0).

%% @deprecated
%% @equiv prepend(PoolPid, Key, Value)
%% @doc Deprecated prepend function which accepts an _unused_ CAS value
-spec prepend(pid(), integer(), key(), value()) -> ok | {error, _}.
prepend(PoolPid, _Cas, Key, Value) ->
    prepend(PoolPid, Key, Value).

-spec prepend(pid(), key(), value()) -> ok | {error, _}.
prepend(PoolPid, Key, Value) ->
    store(PoolPid, prepend, Key, Value, none, 0, 0).

%% @doc Touch (set expiration time) on the given key
%% PoolPid libcouchbase instance to use
%% Key key to touch
%% ExpTime a new expiration time for the item
-spec touch(pid(), key(), integer()) -> {ok, any()}.
touch(PoolPid, Key, ExpTime) ->
    {ok, Return} = mtouch(PoolPid, [Key], [ExpTime]),
    {ok, hd(Return)}.

-spec mtouch(pid(), [key()], integer() | [integer()])
        -> {ok, any()} | {error, any()}.
mtouch(PoolPid, Keys, ExpTime) when is_integer(ExpTime) ->
    mtouch(PoolPid, Keys, [ExpTime]);
mtouch(PoolPid, Keys, ExpTimes) ->
    ExpTimesE = case length(Keys) - length(ExpTimes) of
                    R when R > 0 ->
                        ExpTimes ++ lists:duplicate(R, lists:last(ExpTimes));
                    _ ->
                        ExpTimes
                end,
    execute(PoolPid, {mtouch, Keys, ExpTimesE}).

incr(PoolPid, Key, OffSet) ->
    arithmetic(PoolPid, Key, OffSet, 0, 0, 0).

incr(PoolPid, Key, OffSet, Default) ->
    arithmetic(PoolPid, Key, OffSet, 0, 1, Default).

incr(PoolPid, Key, OffSet, Default, Exp) ->
    arithmetic(PoolPid, Key, OffSet, Exp, 1, Default).

decr(PoolPid, Key, OffSet) ->
    arithmetic(PoolPid, Key, -OffSet, 0, 0, 0).

decr(PoolPid, Key, OffSet, Default) ->
    arithmetic(PoolPid, Key, -OffSet, 0, 1, Default).

decr(PoolPid, Key, OffSet, Default, Exp) ->
    arithmetic(PoolPid, Key, -OffSet, Exp, 1, Default).

%%%%%%%%%%%%%%%%%%%%%%%%%
%%% RETRIEVAL METHODS %%%
%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_and_touch(pid(), key(), integer()) -> [{ok, integer(), value()} | {error, _}].
get_and_touch(PoolPid, Key, Exp) ->
    mget(PoolPid, [Key], Exp).

-spec get(pid(), key()) -> {ok, integer(), value()} | {error, _}.
get(PoolPid, Key) ->
    case mget(PoolPid, [Key], 0) of
        {error, _} = E -> E;
        Result -> hd(Result)
    end.

mget(PoolPid, Keys) ->
    mget(PoolPid, Keys, 0).

-spec get_and_lock(pid(), key(), integer()) -> {ok, integer(), value()} | {error, _}.
get_and_lock(PoolPid, Key, Exp) ->
    hd(getl(PoolPid, Key, Exp)).

-spec unlock(pid(), key(), integer()) -> ok | {error, _}.
unlock(PoolPid, Key, Cas) ->
    execute(PoolPid, {unlock, Key, Cas}).

%% @doc main store function takes care of all storing
%% Instance libcouchbase instance to use
%% Op add | replace | set | append | prepend
%%          add : Add the item to the cache, but fail if the object exists already
%%          replace: Replace the existing object in the cache
%%          set : Unconditionally set the object in the cache
%%          append/prepend : Append/Prepend this object to the existing object
%% Key the key to set
%% Value the value to set
%% Transcoder to encode the value
%% Exp When the object should expire. The expiration time is
%%     either an offset into the future.. OR an absolute
%%     timestamp, depending on how large (numerically) the
%%     expiration is. if the expiration exceeds 30 days
%%     (i.e. 24 * 3600 * 30) then it's an absolute timestamp.
%%     pass 0 for infinity
%% CAS
-spec store(pid(), operation_type(), key(), value(), atom(),
    integer(), integer()) -> ok | {error, _}.
store(PoolPid, Op, Key, Value, TranscoderOpts, Exp, Cas) ->
    execute(PoolPid, {store, Op, Key, Value,
        TranscoderOpts, Exp, Cas}).

%% @doc get the value for the given key
%% Instance libcouchbase instance to use
%% HashKey the key to use for hashing
%% Key the key to get
%% Exp When the object should expire
%%      pass a negative number for infinity
-spec mget(pid(), [key()], integer()) -> list().
mget(PoolPid, Keys, Exp) ->
    execute(PoolPid, {mget, Keys, Exp, 0}).

%% @doc Get an item with a lock that has a timeout
%% Instance libcouchbase instance to use
%%  HashKey the key to use for hashing
%%  Key the key to get
%%  Exp When the lock should expire
-spec getl(pid(), key(), integer()) -> list().
getl(PoolPid, Key, Exp) ->
    execute(PoolPid, {mget, [Key], Exp, 1}).

%% @doc perform an arithmetic operation on the given key
%% Instance libcouchbase instance to use
%% Key key to perform on
%% Delta The amount to add / subtract
%% Exp When the object should expire
%% Create set to true if you want the object to be created if it
%%        doesn't exist.
%% Initial The initial value of the object if we create it
-spec arithmetic(pid(), key(), integer(), integer(), integer(), integer()) ->
    ok | {error, _}.
arithmetic(PoolPid, Key, OffSet, Exp, Create, Initial) ->
    execute(PoolPid, {arithmetic, Key, OffSet, Exp, Create, Initial}).

%% @doc remove the value for given key
%% Instance libcouchbase instance to use
%% Key key to  remove
-spec remove(pid(), key()) -> ok | {error, _}.
remove(PoolPid, Key) ->
    execute(PoolPid, {remove, Key, 0}).

%% @doc flush all documents from the bucket
%% Instance libcouchbase Instance to use
%% BucketName name of the bucket to flush
-spec flush(pid(), string()) -> ok | {error, _}.
flush(PoolPid, BucketName) ->
    FlushMarker = <<"__flush_marker_document__">>,
    set(PoolPid, FlushMarker, 0, ""),
    Path = string:join(["pools/default/buckets", BucketName, "controller/doFlush"], "/"),
    Result = http(PoolPid, Path, "", "application/json", post, management),
    handle_flush_result(PoolPid, FlushMarker, Result).

%% @doc flush all documents from the current bucket
%% Instance libcouchbase Instance to use
-spec flush(pid()) -> ok | {error, _}.
flush(PoolPid) ->
    {ok, BucketName} = execute(PoolPid, bucketname),
    flush(PoolPid, BucketName).

handle_flush_result(_, _, {ok, 200, _}) -> ok;
handle_flush_result(PoolPid, FlushMarker, Result={ok, 201, _}) ->
    case get(PoolPid, FlushMarker) of
        {_, {error, key_enoent}} -> ok;
        _ ->
            erlang:send_after(1000, self(), check_flush_done),
            receive
                check_flush_done -> handle_flush_result(PoolPid, FlushMarker, Result)
            end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%           VIEWS           %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc execute a command with the REST API
%% PoolPid pid of connection pool
%% Path HTTP path
%% Body HTTP body (for POST requests)
%% ContentType HTTP content type
%% Method HTTP method
%% Type Couchbase request type
-spec http(pid(), string(), string(), string(), http_method(), http_type())
        -> {ok, binary()} | {error, _}.
http(PoolPid, Path, Body, ContentType, Method, Type) ->
    execute(PoolPid, {http, Path, Body, ContentType, http_method(Method), http_type(Type)}).

%% @doc Query a view
%% PoolPid pid of connection pool
%% DocName design doc name
%% ViewName view name
%% Args arguments and filters (limit etc.)
view(PoolPid, DocName, ViewName, Args) ->
    Path = string:join(["_design", DocName, "_view", ViewName], "/"),
    Resp = case proplists:get_value(keys, Args) of
               undefined ->
                   http(PoolPid, string:join([Path, query_args(Args)], "?"), "", "application/json", get, view);
               Keys ->
                   http(PoolPid, string:join([Path, query_args(proplists:delete(keys, Args))], "?"), binary_to_list(iolist_to_binary(jiffy:encode({[{keys, Keys}]}))), "application/json", post, view)
           end,
    decode_query_resp(Resp).

foldl(Func, Acc, {PoolPid, DocName, ViewName, Args}) ->
    case view(PoolPid, DocName, ViewName, Args) of
        {ok, {_TotalRows, Rows}} ->
            lists:foldl(Func, Acc, Rows);
        {error, _} = E -> E
    end.

foldr(Func, Acc, {PoolPid, DocName, ViewName, Args}) ->
    case view(PoolPid, DocName, ViewName, Args) of
        {ok, {_TotalRows, Rows}} ->
            lists:foldr(Func, Acc, Rows);
        {error, _} = E -> E
    end.

foreach(Func, {PoolPid, DocName, ViewName, Args}) ->
    case view(PoolPid, DocName, ViewName, Args) of
        {ok, {_TotalRows, Rows}} ->
            lists:foreach(Func, Rows);
        {error, _} = E -> E
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% DESIGN DOCUMENT MANAGMENT %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

set_design_doc(PoolPid, DocName, DesignDoc) ->
    Path = string:join(["_design", DocName], "/"),
    Resp = http(PoolPid, Path, binary_to_list(iolist_to_binary(jiffy:encode(DesignDoc))), "application/json", put, view),
    decode_update_design_doc_resp(Resp).

remove_design_doc(PoolPid, DocName) ->
    Path = string:join(["_design", DocName], "/"),
    Resp = http(PoolPid, Path, "", "application/json", delete, view),
    decode_update_design_doc_resp(Resp).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%    INTERNAL FUNCTIONS     %%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

execute(PoolPid, Cmd) ->
    %%?info("EXEC ~p ~p ~p", [PoolPid, process_info(PoolPid), Cmd]),
    gen_server:call(PoolPid, Cmd).

http_type(view) -> 0;
http_type(management) -> 1;
http_type(raw) -> 2.

http_method(get) -> 0;
http_method(post) -> 1;
http_method(put) -> 2;
http_method(delete) -> 3.

query_args(Args) when is_list(Args) ->
    string:join([query_arg(A) || A <- Args], "&").

decode_query_resp({ok, _, Resp}) ->
    case jiffy:decode(Resp) of
        {[{<<"total_rows">>, TotalRows}, {<<"rows">>, Rows}]} ->
            {ok, {TotalRows, lists:map(fun ({Row}) -> Row end, Rows)}};
        {[{<<"rows">>, Rows}]} ->
            {ok, {lists:map(fun ({Row}) -> Row end, Rows)}};
        {[{<<"error">>,Error}, {<<"reason">>, Reason}]} ->
            {error, {view_error(Error), Reason}}
    end;
decode_query_resp({error, _} = E) -> E.

decode_update_design_doc_resp({ok, Http_Code, _Resp}) when 200 =< Http_Code andalso Http_Code < 300 -> ok;
decode_update_design_doc_resp({ok, _Http_Code, Resp}) ->
    case jiffy:decode(Resp) of
        {[{<<"error">>,Error}, {<<"reason">>, Reason}]} ->
            {error, {view_error(Error), Reason}};
        _Other -> {error, {unknown_error, Resp}}
    end.

query_arg({descending, true}) -> "descending=true";
query_arg({descending, false}) -> "descending=false";

query_arg({endkey, V}) when is_list(V) -> string:join(["endkey", V], "=");

query_arg({endkey_docid, V}) when is_list(V) -> string:join(["endkey_docid", V], "=");

query_arg({full_set, true}) -> "full_set=true";
query_arg({full_set, false}) -> "full_set=false";

query_arg({group, true}) -> "group=true";
query_arg({group, false}) -> "group=false";

query_arg({group_level, V}) when is_integer(V) -> string:join(["group_level", integer_to_list(V)], "=");

query_arg({inclusive_end, true}) -> "inclusive_end=true";
query_arg({inclusive_end, false}) -> "inclusive_end=false";

query_arg({key, V}) -> string:join(["key", binary_to_list(iolist_to_binary(jiffy:encode(V)))], "=");

query_arg({keys, V}) when is_list(V) -> string:join(["keys", jiffy:encode(V)], "=");

query_arg({limit, V}) when is_integer(V) -> string:join(["limit", integer_to_list(V)], "=");

query_arg({on_error, continue}) -> "on_error=continue";
query_arg({on_error, stop}) -> "on_error=stop";

query_arg({reduce, true}) -> "reduce=true";
query_arg({reduce, false}) -> "reduce=false";

query_arg({skip, V}) when is_integer(V) -> string:join(["skip", integer_to_list(V)], "=");

query_arg({stale, false}) -> "stale=false";
query_arg({stale, ok}) -> "stale=ok";
query_arg({stale, update_after}) -> "stale=update_after";

query_arg({startkey, V}) when is_list(V) -> string:join(["startkey", V], "=");

query_arg({startkey_docid, V}) when is_list(V) -> string:join(["startkey_docid", V], "=").

view_error(Error) -> list_to_atom(binary_to_list(Error)).
