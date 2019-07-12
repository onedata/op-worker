%%%--------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% This is module that mocks provider qos - it should be deleted right after
%%% implementation of storage qos will be ready.
%%%
%%% @end
%%%--------------------------------------------------------------------
-module(providers_qos).
-author("Michal Cwiertnia").

%% API
-export([get_storage_qos/2]).

-define(P1_QOS, #{
    <<"country">> => <<"PL">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t3">>
}).

-define(P2_QOS, #{
    <<"country">> => <<"FR">>,
    <<"type">> => <<"tape">>,
    <<"tier">> => <<"t2">>
}).

-define(P3_QOS, #{
    <<"country">> => <<"PR">>,
    <<"type">> => <<"disk">>,
    <<"tier">> => <<"t2">>
}).

-define(ALL_QOS, #{
    <<"dev-oneprovider-krakow">> => ?P1_QOS,
    <<"dev-oneprovider-paris">> =>?P2_QOS,
    <<"dev-oneprovider-lisbon">> => ?P3_QOS}).


get_storage_qos(StorageId, StorageSet) ->
    get_provider_qos(StorageId).


get_provider_qos(ProviderId) ->
    {ok, ProvName} = provider_logic:get_name(ProviderId),
    maps:get(ProvName, ?ALL_QOS).
