%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Handler for file attribute read and modification.
%%% @end
%%%--------------------------------------------------------------------
-module(attributes).
-author("Tomasz Lichon").

-include("global_definitions.hrl").
-include("http/http_common.hrl").
-include_lib("cluster_worker/include/modules/datastore/datastore.hrl").
-include("modules/datastore/datastore_specific_models_def.hrl").
-include_lib("ctool/include/logging.hrl").
-include("http/rest/http_status.hrl").

%% API
-export([rest_init/2, terminate/3, allowed_methods/2, is_authorized/2,
    content_types_provided/2, content_types_accepted/2]).

%% resource functions
-export([get_file_attributes/2, set_file_attribute/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:rest_init/2
%%--------------------------------------------------------------------
-spec rest_init(req(), term()) -> {ok, req(), term()} | {shutdown, req()}.
rest_init(Req, _Opts) ->
    {ok, Req, #{}}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:terminate/3
%%--------------------------------------------------------------------
-spec terminate(Reason :: term(), req(), #{}) -> ok.
terminate(_, _, _) ->
    ok.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:allowed_methods/2
%%--------------------------------------------------------------------
-spec allowed_methods(req(), #{} | {error, term()}) -> {[binary()], req(), #{}}.
allowed_methods(Req, State) ->
    {[<<"GET">>, <<"PUT">>], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:is_authorized/2
%%--------------------------------------------------------------------
-spec is_authorized(req(), #{}) -> {true | {false, binary()} | halt, req(), #{}}.
is_authorized(Req, State) ->
    onedata_auth_api:is_authorized(Req, State).

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_provided/2
%%--------------------------------------------------------------------
-spec content_types_provided(req(), #{}) -> {[{binary(), atom()}], req(), #{}}.
content_types_provided(Req, State) ->
    {[
        {<<"application/json">>, get_file_attributes}
    ], Req, State}.

%%--------------------------------------------------------------------
%% @doc @equiv pre_handler:content_types_accepted/2
%%--------------------------------------------------------------------
-spec content_types_accepted(req(), #{}) ->
    {[{binary(), atom()}], req(), #{}}.
content_types_accepted(Req, State) ->
    {[
        {<<"application/json">>, set_file_attribute}
    ], Req, State}.


%%%===================================================================
%%% Content type handler functions
%%%===================================================================

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/attributes/{path}'
%% @doc This method returns selected file attributes.
%%
%% HTTP method: GET
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param attribute Type of attribute to query for.
%%--------------------------------------------------------------------
-spec get_file_attributes(req(), #{}) -> {term(), req(), #{}}.
get_file_attributes(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_extended(Req2, State2),
    {State4, Req4} = validator:parse_attribute(Req3, State3),

    #{auth := Auth, path := Path, attribute := Attribute, extended := Extended} = State4,

    case {Attribute, Extended} of
        {ModeOrUndefined, false} when ModeOrUndefined =:= <<"mode">> ; ModeOrUndefined =:= undefined ->
            {ok, #file_attr{mode = Mode}} = onedata_file_api:stat(Auth, {path, Path}),
            Response = json_utils:encode([[{<<"name">>, <<"mode">>}, {<<"value">>, <<"0", (integer_to_binary(Mode, 8))/binary>>}]]),
            {Response, Req4, State4};
        {undefined, true} ->
            {ok, Xattrs} = onedata_file_api:list_xattr(Auth, {path, Path}),
            RawResponse = lists:map(fun(XattrName) ->
                {ok, #xattr{value = Value}} = onedata_file_api:get_xattr(Auth, {path, Path}, XattrName),
                [{<<"name">>, XattrName}, {<<"value">>, Value}]
            end, Xattrs),
            Response = json_utils:encode(RawResponse),
            {Response, Req4, State4};
        {XattrName, true} ->
            {ok, #xattr{value = Value}} = onedata_file_api:get_xattr(Auth, {path, Path}, XattrName),
            Response = json_utils:encode([[{<<"name">>, XattrName}, {<<"value">>, Value}]]),
            {Response, Req4, State4}
    end.

%%--------------------------------------------------------------------
%% '/api/v3/oneprovider/attributes/{path}'
%% @doc This method allows to set a value of a specific file attribute (e.g. mode).
%%
%% HTTP method: PUT
%%
%% @param path File path (e.g. &#39;/My Private Space/testfiles/file1.txt&#39;)
%% @param attribute Attribute name and value.
%%--------------------------------------------------------------------
-spec set_file_attribute(req(), #{}) -> {term(), req(), #{}}.
set_file_attribute(Req, State) ->
    {State2, Req2} = validator:parse_path(Req, State),
    {State3, Req3} = validator:parse_extended(Req2, State2),
    {State4, Req4} = validator:parse_attribute_body(Req3, State3),

    #{attribute_body := {Attribute, Value}, path := Path, auth := Auth, extended := Extended} = State4,

    case {Attribute, Extended} of
        {<<"mode">>, false} ->
            ok = onedata_file_api:set_perms(Auth, {path, Path}, Value);
        {_, true} ->
            ok = onedata_file_api:set_xattr(Auth, {path, Path}, #xattr{name = Attribute, value = Value})
    end,
    {true, Req4, State4}.

%%%===================================================================
%%% Internal functions
%%%===================================================================