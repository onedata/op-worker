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

-include("modules/datastore/datastore_model.hrl").
-include_lib("ctool/include/logging.hrl").

%% API
-export([get_by_name/2, delete_by_name/2, exists_by_name/2, save/2, list/1]).

%% model_behaviour callbacks
-export([save/1, get/1, exists/1, delete/1, update/2, create/1, model_init/0,
    'after'/5, before/4]).
-export([run_synchronized/2]).

-type name() :: binary().
-type value() :: binary().

-export_type([name/0, value/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Gets extended attribute with given name
%%--------------------------------------------------------------------
-spec get_by_name(file_meta:uuid(), xattr:name()) ->
    {ok, #xattr{}} | datastore:generic_error().
get_by_name(FileUuid, XattrName) ->
    xattr:get({FileUuid, XattrName}).

%%--------------------------------------------------------------------
%% @doc Deletes extended attribute with given name
%%--------------------------------------------------------------------
-spec get_by_name(file_meta:uuid(), xattr:name()) ->
    ok | datastore:generic_error().
delete_by_name(FileUuid, XattrName) ->
    xattr:delete({FileUuid, XattrName}).

%%--------------------------------------------------------------------
%% @doc Checks existence of extended attribute with given name
%%--------------------------------------------------------------------
-spec exists_by_name(file_meta:uuid(), xattr:name()) -> datastore:exists_return().
exists_by_name(FileUuid, XattrName) ->
    xattr:exists({FileUuid, XattrName}).

%%--------------------------------------------------------------------
%% @doc Saves extended attribute
%%--------------------------------------------------------------------
-spec save(file_meta:uuid(), #xattr{}) ->
    {ok, datastore:key()} | datastore:generic_error().
save(FileUuid, Xattr = #xattr{name = XattrKey}) ->
    save(#document{key = {FileUuid, XattrKey}, value = Xattr}).

%%--------------------------------------------------------------------
%% @doc Lists names of all extended attributes associated with given file
%%--------------------------------------------------------------------
-spec exists_by_name(file_meta:uuid(), xattr:name()) ->
    {ok, [xattr:name()]} | datastore:generic_error().
list(FileUuid) ->
    datastore:foreach_link(?LINK_STORE_LEVEL, FileUuid, ?MODEL_NAME,
        fun(Name, _, Acc) ->
            [Name | Acc]
        end, []).

%%%===================================================================
%%% model_behaviour callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Runs given function within locked ResourceId. This function makes sure that 2 funs with same ResourceId won't
%% run at the same time.
%% @end
%%--------------------------------------------------------------------
-spec run_synchronized(ResourceId :: binary(), Fun :: fun(() -> Result :: term())) -> Result :: term().
run_synchronized(ResourceId, Fun) ->
    datastore:run_synchronized(?MODEL_NAME, ResourceId, Fun).

%%--------------------------------------------------------------------
%% @doc
%% {@link model_behaviour} callback save/1.
%% @end
%%--------------------------------------------------------------------
-spec save(datastore:document()) ->
    {ok, datastore:key()} | datastore:generic_error().
save(Document = #document{key = {Uuid, Name}}) ->
    case datastore:save(?STORE_LEVEL, Document) of
        {ok, Key}->
            ok = datastore:add_links(?LINK_STORE_LEVEL, Uuid, ?MODEL_NAME, {Name, {Key, ?MODEL_NAME}}),
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
create(Document = #document{key = {Uuid, Name}}) ->
    case datastore:create(?STORE_LEVEL, Document) of
        {ok, Key}->
            ok = datastore:add_links(?LINK_STORE_LEVEL, Uuid, ?MODEL_NAME, {Name, {Key, ?MODEL_NAME}}),
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
delete({Uuid, Name} = Key) ->
    case datastore:delete(?STORE_LEVEL, ?MODULE, Key) of
        ok ->
            datastore:delete_links(?LINK_STORE_LEVEL, Uuid, {Name, {Key, ?MODEL_NAME}});
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
