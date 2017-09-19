%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding database views.
%%% @end
%%%-------------------------------------------------------------------
-module(indexes).
-author("Tomasz Lichon").

-include("modules/datastore/datastore_models.hrl").
-include("modules/datastore/datastore_runner.hrl").
-include_lib("ctool/include/posix/errors.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([add_index/5, get_index/2, remove_index/2, query_view/2,
    get_all_indexes/1, change_index_function/3, save/1, get/1, exists/1,
    delete/1, update/2, create/1, create_or_update/2]).

%% datastore_model callbacks
-export([get_record_struct/1]).

-type key() :: datastore:key().
-type record() :: #indexes{}.
-type doc() :: datastore_doc:doc(record()).
-type diff() :: datastore_doc:diff(record()).
-type view_name() :: binary().
-type index_id() :: binary().
-type view_function() :: binary().
% when set to true, view is of spatial type. It should emit geospatial json keys,
% and can be queried with use of bbox, start_range, end_range params. More info
% here: http://developer.couchbase.com/documentation/server/current/indexes/writing-spatial-views.html
-type spatial() :: boolean().
-type index() :: #{name => indexes:view_name(),
space_id => od_space:id(),
function => indexes:view_function(),
spatial => indexes:spatial()}.

-export_type([view_name/0, index_id/0, view_function/0, spatial/0]).

-define(CTX, #{model => ?MODULE}).
-define(DISC_CTX, #{bucket => <<"onedata">>}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @equiv add_index(UserId, ViewName, ViewFunction, SpaceId, Spatial, datastore_utils:gen_uuid()).
%%--------------------------------------------------------------------
-spec add_index(od_user:id(), view_name(), view_function(), od_space:id(), spatial()) ->
    {ok, index_id()} | {error, any()}.
add_index(UserId, ViewName, ViewFunction, SpaceId, Spatial) ->
    add_index(UserId, ViewName, ViewFunction, SpaceId, Spatial, datastore_utils:gen_key()).

%%--------------------------------------------------------------------
%% @doc
%% Add or update view defined by given function.
%% @end
%%--------------------------------------------------------------------
-spec add_index(od_user:id(), view_name() | undefined, view_function(),
    od_space:id() | undefined, spatial() | undefined, index_id()) -> {ok, index_id()} | {error, any()}.
add_index(UserId, ViewName, ViewFunction, SpaceId, Spatial, IndexId) ->
    EscapedViewFunction = escape_js_function(ViewFunction),
    critical_section:run([?MODULE, UserId], fun() ->
        case indexes:get(UserId) of
            {ok, Doc = #document{value = IndexesDoc = #indexes{value = Indexes}}} ->
                NewMap =
                    case maps:get(IndexId, Indexes, undefined) of
                        undefined ->
                            add_db_view(IndexId, SpaceId, EscapedViewFunction, Spatial),
                            maps:put(IndexId, #{name => ViewName, space_id => SpaceId, function => EscapedViewFunction, spatial => Spatial}, Indexes);
                        OldMap = #{space_id := SId, spatial := DefinedSpatial} when SpaceId =:= undefined ->
                            add_db_view(IndexId, SId, EscapedViewFunction, DefinedSpatial),
                            maps:put(IndexId, OldMap#{function => EscapedViewFunction}, Indexes);
                        OldMap = #{spatial := DefinedSpatial} ->
                            add_db_view(IndexId, SpaceId, EscapedViewFunction, DefinedSpatial),
                            maps:put(IndexId, OldMap#{function => EscapedViewFunction, space_id => SpaceId}, Indexes)
                    end,
                case save(Doc#document{value = IndexesDoc#indexes{value = NewMap}}) of
                    {ok, _} ->
                        {ok, IndexId};
                    Error ->
                        Error
                end;
            {error, not_found} ->
                add_db_view(IndexId, SpaceId, EscapedViewFunction, Spatial),
                Map = maps:put(IndexId, #{name => ViewName, space_id => SpaceId, function => EscapedViewFunction, spatial => Spatial}, #{}),
                case create(#document{key = UserId, value = #indexes{value = Map}}) of
                    {ok, _} ->
                        {ok, IndexId};
                    Error ->
                        Error
                end;
            Error ->
                Error
        end
    end).

%%--------------------------------------------------------------------
%% @doc
%% Get index from db.
%% @end
%%--------------------------------------------------------------------
-spec get_index(od_user:id(), index_id()) -> {ok, index()} | {error, any()}.
get_index(UserId, IndexId) ->
    case indexes:get(UserId) of
        {ok, #document{value = #indexes{value = Indexes}}} ->
            case maps:get(IndexId, Indexes, undefined) of
                undefined ->
                    {error, ?ENOENT};
                Val ->
                    {ok, Val#{id => IndexId}}
            end;
        {error, not_found} ->
            {error, ?ENOENT};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Remove index from db
%% @end
%%--------------------------------------------------------------------
-spec remove_index(od_user:id(), index_id()) -> ok | {error, any()}.
remove_index(UserId, IndexId) ->
    case update(UserId, fun(IndexesDoc = #indexes{value = Indexes}) ->
        NewValue = maps:remove(IndexId, Indexes),
        {ok, IndexesDoc#indexes{value = NewValue}}
    end) of
        {ok, _} ->
            couchbase_driver:delete_design_doc(?DISC_CTX, IndexId);
        {error, not_found} ->
            ok;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Change index js function.
%% @end
%%--------------------------------------------------------------------
-spec change_index_function(od_user:id(), index_id(), view_function()) -> {ok, index_id()} | {error, any()}.
change_index_function(UserId, IndexId, Function) ->
    add_index(UserId, undefined, Function, undefined, undefined, IndexId).

%%--------------------------------------------------------------------
%% @doc
%% Get all indexes from db, with given space id.
%% @end
%%--------------------------------------------------------------------
-spec get_all_indexes(od_user:id()) -> {ok, maps:map()} | {error, any()}.
get_all_indexes(UserId) ->
    case indexes:get(UserId) of
        {ok, #document{value = #indexes{value = Indexes}}} ->
            {ok, Indexes};
        {error, not_found} ->
            {ok, #{}};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get view result.
%% @end
%%--------------------------------------------------------------------
-spec query_view(index_id(), list()) -> {ok, [file_meta:uuid()]}.
query_view(Id, Options) ->
    {ok, {Rows}} = couchbase_driver:query_view(?DISC_CTX, Id, Id, Options),
    FileUuids = lists:map(fun(Row) ->
        {<<"value">>, FileUuid} = lists:keyfind(<<"value">>, 1, Row),
        FileUuid
    end, Rows),
    {ok, lists:filtermap(fun(Uuid) ->
        try
            {true, fslogic_uuid:uuid_to_guid(Uuid)}
        catch
            _:Error ->
                ?error("Cannot resolve uuid of file ~p in index ~p, error ~p", [Uuid, Id, Error]),
                false
        end
    end, FileUuids)}.

%%--------------------------------------------------------------------
%% @doc
%% Saves index.
%% @end
%%--------------------------------------------------------------------
-spec save(doc()) -> {ok, key()} | {error, term()}.
save(Doc) ->
    ?extract_key(datastore_model:save(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates index.
%% @end
%%--------------------------------------------------------------------
-spec update(key(), diff()) -> {ok, key()} | {error, term()}.
update(Key, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff)).

%%--------------------------------------------------------------------
%% @doc
%% Creates index.
%% @end
%%--------------------------------------------------------------------
-spec create(doc()) -> {ok, key()} | {error, term()}.
create(Doc) ->
    ?extract_key(datastore_model:create(?CTX, Doc)).

%%--------------------------------------------------------------------
%% @doc
%% Updates document with using ID from document. If such object does not exist,
%% it initialises the object with the document.
%% @end
%%--------------------------------------------------------------------
-spec create_or_update(doc(), diff()) ->
    {ok, key()} | {error, term()}.
create_or_update(#document{key = Key, value = Default}, Diff) ->
    ?extract_key(datastore_model:update(?CTX, Key, Diff, Default)).

%%--------------------------------------------------------------------
%% @doc
%% Returns index.
%% @end
%%--------------------------------------------------------------------
-spec get(key()) -> {ok, doc()} | {error, term()}.
get(Key) ->
    datastore_model:get(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Deletes index.
%% @end
%%--------------------------------------------------------------------
-spec delete(key()) -> ok | {error, term()}.
delete(Key) ->
    datastore_model:delete(?CTX, Key).

%%--------------------------------------------------------------------
%% @doc
%% Checks whether index exists.
%% @end
%%--------------------------------------------------------------------
-spec exists(key()) -> boolean().
exists(Key) ->
    {ok, Exists} = datastore_model:exists(?CTX, Key),
    Exists.

%%%===================================================================
%%% Internal functions
%%%==================================================================

%%--------------------------------------------------------------------
%% @doc
%% Add view to db, Function should be a valid, escaped javascript string
%% with one argument function.
%% @end
%%--------------------------------------------------------------------
-spec add_db_view(index_id(), od_space:id(), view_function(), spatial()) -> ok.
add_db_view(IndexId, SpaceId, Function, Spatial) ->
    RecordName = custom_metadata,
    RecordNameBin = atom_to_binary(RecordName, utf8),
    ViewFunction = <<"function (doc, meta) { 'use strict'; if(doc['_record'] == '", RecordNameBin/binary, "' && doc['space_id'] == '", SpaceId/binary, "') { "
    "var key = eval.call(null, '(", Function/binary, ")'); ",
        "var key_to_emit = key(doc['value']); if(key_to_emit) { emit(key_to_emit, doc['_key']); } } }">>,
    ok = case Spatial of
        true ->
            couchbase_driver:save_spatial_view_doc(?DISC_CTX, IndexId, ViewFunction);
        _ ->
            couchbase_driver:save_view_doc(?DISC_CTX, IndexId, ViewFunction)
    end.

%%--------------------------------------------------------------------
%% @doc escapes characters: \ " ' \n \t \v \0 \f \r
%%--------------------------------------------------------------------
-spec escape_js_function(Function :: binary()) -> binary().
escape_js_function(undefined) ->
    undefined;
escape_js_function(Function) ->
    escape_js_function(Function, [{<<"\\\\">>, <<"\\\\\\\\">>}, {<<"'">>, <<"\\\\'">>},
        {<<"\"">>, <<"\\\\\"">>}, {<<"\\n">>, <<"\\\\n">>}, {<<"\\t">>, <<"\\\\t">>},
        {<<"\\v">>, <<"\\\\v">>}, {<<"\\0">>, <<"\\\\0">>}, {<<"\\f">>, <<"\\\\f">>},
        {<<"\\r">>, <<"\\\\r">>}]).

%%--------------------------------------------------------------------
%% @doc Escapes characters given as proplists, in provided Function.
%%--------------------------------------------------------------------
-spec escape_js_function(Function :: binary(), [{binary(), binary()}]) -> binary().
escape_js_function(Function, []) ->
    Function;
escape_js_function(Function, [{Pattern, Replacement} | Rest]) ->
    EscapedFunction = re:replace(Function, Pattern, Replacement, [{return, binary}, global]),
    escape_js_function(EscapedFunction, Rest).

%%%===================================================================
%%% datastore_model callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Returns model's record structure in provided version.
%% @end
%%--------------------------------------------------------------------
-spec get_record_struct(datastore_model:record_version()) ->
    datastore_model:record_struct().
get_record_struct(1) ->
    {record, [
        {value, #{string => {custom, index, index_encoder}}}
    ]}.