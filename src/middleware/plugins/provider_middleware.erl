%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% This module handles middleware operations (create, get, update, delete)
%%% corresponding to provider aspects such as e.g. instance or configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(provider_middleware).
-author("Bartosz Walkowicz").

-behaviour(middleware_plugin).

-include("middleware/middleware.hrl").
-include("modules/rtransfer/rtransfer.hrl").
-include_lib("ctool/include/errors.hrl").

-export([gather_configuration/0]).

-export([
    operation_supported/3,
    data_spec/1,
    fetch_entity/1,
    authorize/2,
    validate/2
]).
-export([create/1, get/2, update/1, delete/1]).


%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Returns contents of the configuration object.
%% @end
%%--------------------------------------------------------------------
-spec gather_configuration() -> #{binary() := term()}.
gather_configuration() ->
    ProviderId = oneprovider:get_id_or_undefined(),
    Name = case provider_logic:get_name() of
        {ok, N} -> N;
        _ -> undefined
    end,
    Domain = case provider_logic:get_domain() of
        {ok, D} -> D;
        _ -> undefined
    end,
    OnezoneDomain = case ProviderId of
        undefined -> undefined;
        _ -> oneprovider:get_oz_domain()
    end,
    Version = oneprovider:get_version(),
    {ok, CompOzVersions} = compatibility:get_compatible_versions(?ONEPROVIDER, Version, ?ONEZONE),
    {ok, CompOpVersions} = compatibility:get_compatible_versions(?ONEPROVIDER, Version, ?ONEPROVIDER),
    {ok, CompOcVersions} = compatibility:get_compatible_versions(?ONEPROVIDER, Version, ?ONECLIENT),

    #{
        <<"providerId">> => utils:undefined_to_null(ProviderId),
        <<"name">> => utils:undefined_to_null(Name),
        <<"domain">> => utils:undefined_to_null(Domain),
        <<"onezoneDomain">> => utils:undefined_to_null(OnezoneDomain),
        <<"version">> => Version,
        <<"build">> => oneprovider:get_build(),
        <<"rtransferPort">> => ?RTRANSFER_PORT,
        <<"compatibleOnezoneVersions">> => CompOzVersions,
        <<"compatibleOneproviderVersions">> => CompOpVersions,
        <<"compatibleOneclientVersions">> => CompOcVersions
    }.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback operation_supported/3.
%% @end
%%--------------------------------------------------------------------
-spec operation_supported(middleware:operation(), gri:aspect(),
    middleware:scope()) -> boolean().
operation_supported(get, instance, protected) -> true;
operation_supported(get, configuration, public) -> true;
operation_supported(get, test_image, public) -> true;

operation_supported(_, _, _) -> false.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback data_spec/1.
%% @end
%%--------------------------------------------------------------------
-spec data_spec(middleware:req()) -> undefined | middleware_sanitizer:data_spec().
data_spec(#op_req{operation = get, gri = #gri{aspect = instance}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = configuration}}) ->
    undefined;
data_spec(#op_req{operation = get, gri = #gri{aspect = test_image}}) ->
    undefined.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback fetch_entity/1.
%% @end
%%--------------------------------------------------------------------
-spec fetch_entity(middleware:req()) ->
    {ok, middleware:versioned_entity()} | errors:error().
fetch_entity(#op_req{auth = ?USER(_UserId, SessionId), auth_hint = AuthHint, gri = #gri{
    id = ProviderId,
    aspect = instance,
    scope = protected
}}) ->
    case provider_logic:get_protected_data(SessionId, ProviderId, AuthHint) of
        {ok, #document{value = Provider}} ->
            {ok, {Provider, 1}};
        {error, _} = Error ->
            Error
    end;
fetch_entity(_) ->
    {ok, {undefined, 1}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback authorize/2.
%% @end
%%--------------------------------------------------------------------
-spec authorize(middleware:req(), middleware:entity()) -> boolean().
authorize(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    % authorization was checked by oz in `fetch_entity`
    true;
authorize(#op_req{operation = get, gri = #gri{aspect = configuration}}, _) ->
    true;
authorize(#op_req{operation = get, gri = #gri{aspect = test_image}}, _) ->
    true.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback validate/1.
%% @end
%%--------------------------------------------------------------------
-spec validate(middleware:req(), middleware:entity()) -> ok | no_return().
validate(#op_req{operation = get, gri = #gri{aspect = instance}}, _) ->
    % validation was checked by oz in `fetch_entity`
    ok;
validate(#op_req{operation = get, gri = #gri{aspect = configuration}}, _) ->
    ok;
validate(#op_req{operation = get, gri = #gri{aspect = test_image}}, _) ->
    ok.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback create/2.
%% @end
%%--------------------------------------------------------------------
-spec create(middleware:req()) -> middleware:create_result().
create(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback get/2.
%% @end
%%--------------------------------------------------------------------
-spec get(middleware:req(), middleware:entity()) -> middleware:get_result().
get(#op_req{gri = #gri{aspect = instance, scope = protected}}, Provider) ->
    {ok, Provider};
get(#op_req{gri = #gri{aspect = configuration}}, _) ->
    {ok, value, gather_configuration()};
get(#op_req{gri = #gri{aspect = test_image}}, _) ->
    % Dummy image in png format. Used by gui to check connectivity.
    {ok, value, {binary, <<
        137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0,
        0, 1, 0, 0, 0, 1, 1, 3, 0, 0, 0, 37, 219, 86, 202, 0, 0, 0, 6, 80,
        76, 84, 69, 0, 0, 0, 255, 255, 255, 165, 217, 159, 221, 0, 0, 0, 9,
        112, 72, 89, 115, 0, 0, 14, 196, 0, 0, 14, 196, 1, 149, 43, 14, 27,
        0, 0, 0, 10, 73, 68, 65, 84, 8, 153, 99, 96, 0, 0, 0, 2, 0, 1, 244,
        113, 100, 166, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130
    >>}}.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback update/1.
%% @end
%%--------------------------------------------------------------------
-spec update(middleware:req()) -> middleware:update_result().
update(_) ->
    ?ERROR_NOT_SUPPORTED.


%%--------------------------------------------------------------------
%% @doc
%% {@link middleware_plugin} callback delete/1.
%% @end
%%--------------------------------------------------------------------
-spec delete(middleware:req()) -> middleware:delete_result().
delete(_) ->
    ?ERROR_NOT_SUPPORTED.
