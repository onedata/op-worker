%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Cache that maps credentials to users' identities
%%% @end
%%%-------------------------------------------------------------------
-module(identity).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_model.hrl").

-include("proto/oneclient/handshake_messages.hrl").

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1,
    model_init/0, 'after'/5, before/4]).

%% API
-export([fetch/1, get_or_fetch/1]).

-export_type([credentials/0]).

%% todo split this model to:
%% todo globally cached - #certificate{} -> #identity{},
%% todo and locally cached - #token{} | #certificate_info{} -> #identity{}
-type credentials() :: #token{} | #'OTPCertificate'{}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) -> {ok, datastore:ext_key()} | datastore:generic_error().
save(Document) ->
    datastore:save(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback update/2.
%% @end
%%--------------------------------------------------------------------
-spec update(datastore:ext_key(), Diff :: datastore:document_diff()) ->
    {ok, datastore:ext_key()} | datastore:update_error().
update(Key, Diff) ->
    datastore:update(?STORE_LEVEL, ?MODULE, Key, Diff).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback create/1.
%% @end
%%--------------------------------------------------------------------
-spec create(datastore:document()) -> {ok, datastore:ext_key()} | datastore:create_error().
create(Document) ->
    datastore:create(?STORE_LEVEL, Document).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:ext_key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:ext_key()) -> ok | datastore:generic_error().
delete(Key) ->
    datastore:delete(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback exists/1.
%% @end
%%--------------------------------------------------------------------
-spec exists(datastore:ext_key()) -> datastore:exists_return().
exists(Key) ->
    ?RESPONSE(datastore:exists(?STORE_LEVEL, ?MODULE, Key)).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback model_init/0.
%% @end
%%--------------------------------------------------------------------
-spec model_init() -> model_behaviour:model_config().
model_init() ->
    ?MODEL_CONFIG(identity_bucket, [], ?LOCAL_ONLY_LEVEL).

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
%% Fetch user from globalregistry and save it in cache.
%% @end
%%--------------------------------------------------------------------
-spec fetch(identity:credentials()) ->
    {ok, datastore:document()} | datastore:get_error().
fetch(OtpCert = #'OTPCertificate'{}) ->
    case identity:get(OtpCert) of
        {ok, Doc = #document{value = Iden}} ->
            identity:save(#document{key = OtpCert, value = Iden}),
            {ok, Doc};
        Error_ -> Error_
    end;
fetch(Token = #token{}) ->
    case onedata_user:fetch(Token) of
        {ok, #document{key = Id}} ->
            NewDoc = #document{key = Token, value = #identity{user_id = Id}},
            case identity:save(NewDoc) of
                {ok, _} -> {ok, NewDoc};
                Error_ -> Error_
            end;
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get user's identity from cache, or fetch user from globalregistry
%% and store its identity
%% @end
%%--------------------------------------------------------------------
-spec get_or_fetch(identity:credentials()) ->
    {ok, datastore:document()} | datastore:get_error().
get_or_fetch(Cred) ->
    case identity:get(Cred) of
        {ok, Doc} -> {ok, Doc};
        {error, {not_found, _}} -> fetch(Cred);
        Error -> Error
    end.
