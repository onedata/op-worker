%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Session management model, frequently invoked by incoming tcp
%%% connections in connection
%%% @end
%%%-------------------------------------------------------------------
-module(message_id).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include("modules/fslogic/fslogic_common.hrl").
-include("proto/common/credentials.hrl").
-include_lib("ctool/include/logging.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, list/0, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([generate/0, generate/1, generate/2, encode/1, decode/1]).

-export_type([id/0]).

-type id() :: #message_id{}.

-define(INT64, 16#FFFFFFFFFFFFFFF).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:key()} | datastore:generic_error().
save(#document{} = Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:key()} | datastore:create_error().
create(#document{} = Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% Sets access time to current time for user session and returns old value.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% Returns list of all records.
%% @end
%%--------------------------------------------------------------------
-spec list() -> {ok, [datastore:document()]} | datastore:generic_error() | no_return().
list() ->
    datastore:list(?STORE_LEVEL, ?MODEL_NAME, ?GET_ALL, []).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(message_id_bucket, [], ?GLOBAL_ONLY_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(), Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) -> ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @equiv generate(undefined).
%% @end
%%--------------------------------------------------------------------
-spec generate() -> {ok, #message_id{}}.
generate() ->
    generate(undefined).


%%--------------------------------------------------------------------
%% @doc
%% Generates ID with encoded handler pid.
%% @end
%%--------------------------------------------------------------------
-spec generate(Recipient :: pid() | undefined) -> {ok, MsgId :: #message_id{}}.
generate(Recipient) ->
    generate(Recipient, server).


%%--------------------------------------------------------------------
%% @doc
%% Generates ID with encoded handler pid and given issuer type.
%% @end
%%--------------------------------------------------------------------
-spec generate(Recipient :: pid() | undefined, Issuer :: client | server) -> {ok, MsgId :: #message_id{}}.
generate(Recipient, Issuer) ->
    {ok, #message_id{
        issuer = Issuer,
        id = integer_to_binary(crypto:rand_uniform(0, ?INT64)),
        recipient = Recipient
    }}.

%%--------------------------------------------------------------------
%% @doc
%% Encodes message_id to binary form.
%% @end
%%--------------------------------------------------------------------
-spec encode(MsgId :: #message_id{} | undefined) -> {ok, undefined | binary()}.
encode(undefined) ->
    {ok, undefined};
encode(#message_id{issuer = client, id = Id}) ->
    {ok, Id};
encode(MsgId = #message_id{}) ->
    {ok, term_to_binary(MsgId)}.

%%--------------------------------------------------------------------
%% @doc
%% Decodes message_id from binary form.
%% @end
%%--------------------------------------------------------------------
-spec decode(Id :: binary()) -> {ok, #message_id{}}.
decode(undefined) ->
    {ok, undefined};
decode(Id) ->
    try binary_to_term(Id) of
        #message_id{} = MsgId ->
            {ok, MsgId};
        _ ->
            {ok, #message_id{issuer = client, id = Id}}
    catch
        _:_ ->
            {ok, #message_id{issuer = client, id = Id}}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
