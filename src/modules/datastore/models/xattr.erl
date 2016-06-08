%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc Model for holding files' extended attributes.
%%% @end
%%%-------------------------------------------------------------------
-module(xattr).
-author("Tomasz Lichon").
-behaviour(model_behaviour).

-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_by_name/2, delete_by_name/2, exists_by_name/2, save/2, list/1,
    get_file_uuid/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).

-type name() :: binary().
-type value() :: binary().
-type transfer_encoding() :: binary(). % <<"utf-8">> | <<"base64">>
-type cdmi_completion_status() :: binary(). % <<"Completed">> | <<"Processing">> | <<"Error">>
-type mimetype() :: binary().

-export_type([name/0, value/0, transfer_encoding/0, cdmi_completion_status/0, mimetype/0]).

-define(XATTR_LINK_NAME(XattrName), <<"xattr_", XattrName/binary>>).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets extended attribute with given name
%%--------------------------------------------------------------------
-spec get_by_name(file_meta:uuid(), xattr:name()) ->
    {ok, datastore:document()} | datastore:get_error().
get_by_name(FileUuid, XattrName) ->
    xattr:get(encode_key(FileUuid, XattrName)).

%%--------------------------------------------------------------------
%% @doc Deletes extended attribute with given name
%%--------------------------------------------------------------------
-spec delete_by_name(file_meta:uuid(), xattr:name()) ->
    ok | datastore:generic_error().
delete_by_name(FileUuid, XattrName) ->
    xattr:delete(encode_key(FileUuid, XattrName)).

%%--------------------------------------------------------------------
%% @doc Checks existence of extended attribute with given name
%%--------------------------------------------------------------------
-spec exists_by_name(file_meta:uuid(), xattr:name()) -> datastore:exists_return().
exists_by_name(FileUuid, XattrName) ->
    xattr:exists(encode_key(FileUuid, XattrName)).

%%--------------------------------------------------------------------
%% @doc Saves extended attribute
%%--------------------------------------------------------------------
-spec save(file_meta:uuid(), #xattr{}) ->
    {ok, datastore:key()} | datastore:generic_error().
save(FileUuid, Xattr = #xattr{name = XattrName}) ->
    save(#document{key = encode_key(FileUuid, XattrName), value = Xattr}).

%%--------------------------------------------------------------------
%% @doc Lists names of all extended attributes associated with given file
%%--------------------------------------------------------------------
-spec list(file_meta:uuid()) -> {ok, [xattr:name()]} | datastore:generic_error().
list(FileUuid) ->
    datastore:foreach_link(?LINK_STORE_LEVEL, FileUuid, ?MODEL_NAME,
        fun
            (?XATTR_LINK_NAME(Name), _, Acc) ->
                [Name | Acc];
            (_, _, Acc) ->
                Acc
        end, []).

%%--------------------------------------------------------------------
%% @doc Get file Uuid from xattr uuid
%%--------------------------------------------------------------------
-spec get_file_uuid(datastore:key()) -> {ok, file_meta:uuid()}.
get_file_uuid(XattrUuid) ->
    {Uuid, _Name} = decode_key(XattrUuid),
    {ok, Uuid}.

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document) ->
    case datastore:save(?STORE_LEVEL, Document) of
        {ok, Key} ->
            {Uuid, Name} = decode_key(Key),
            ok = datastore:add_links(?LINK_STORE_LEVEL, Uuid, ?MODEL_NAME, {?XATTR_LINK_NAME(Name), {Key, ?MODEL_NAME}}),
            {ok, Key};
        Error ->
            Error
    end.

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
-spec create(datastore:document()) ->
    {ok, datastore:key()} | datastore:create_error().
create(Document) ->
    case datastore:create(?STORE_LEVEL, Document) of
        {ok, Key}->
            {Uuid, Name} = decode_key(Key),
            ok = datastore:add_links(?LINK_STORE_LEVEL, Uuid, ?MODEL_NAME, {?XATTR_LINK_NAME(Name), {Key, ?MODEL_NAME}}),
            {ok, Key};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback get/1.
%% @end
%%--------------------------------------------------------------------
-spec get(datastore:key()) -> {ok, datastore:document()} | datastore:get_error().
get(Key) ->
    datastore:get(?STORE_LEVEL, ?MODULE, Key).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(datastore:key()) -> ok | datastore:generic_error().
delete(Key) ->
    case datastore:delete(?STORE_LEVEL, ?MODULE, Key) of
        ok ->
            {Uuid, Name} = decode_key(Key),
            ok = datastore:delete_links(?LINK_STORE_LEVEL, Uuid, ?MODEL_NAME, ?XATTR_LINK_NAME(Name));
        Error ->
            Error
    end.

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
    ?MODEL_CONFIG(xattr_bucket, [], ?GLOBALLY_CACHED_LEVEL).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback 'after'/5.
%% @end
%%--------------------------------------------------------------------
-spec 'after'(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term(),
    ReturnValue :: term()) -> ok.
'after'(_ModelName, _Method, _Level, _Context, _ReturnValue) ->
    ok.

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback before/4.
%% @end
%%--------------------------------------------------------------------
-spec before(ModelName :: model_behaviour:model_type(),
    Method :: model_behaviour:model_action(),
    Level :: datastore:store_level(), Context :: term()) ->
    ok | datastore:generic_error().
before(_ModelName, _Method, _Level, _Context) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Decodes xattr key to file uuid and xattr name
%%--------------------------------------------------------------------
-spec decode_key(binary()) -> {file_meta:uuid(), xattr:name()}.
decode_key(Key) ->
    binary_to_term(Key).

%%--------------------------------------------------------------------
%% @doc Creates xattr key from file uuid and xattr name
%%--------------------------------------------------------------------
-spec encode_key(file_meta:uuid(), xattr:name()) -> binary().
encode_key(FileUuid, XattrName) ->
    term_to_binary({FileUuid, XattrName}).