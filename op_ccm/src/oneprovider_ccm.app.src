%%%-------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% It is the description of oneprovider_ccm application.
%%% @end
%%%-------------------------------------------------------------------
{application, oneprovider_ccm, [
    {description, "Application starts central manager of oneprovider cluster"},
    {vsn, git},
    {registered, [oneprovider_ccm_sup]},
    {applications, [
        kernel,
        stdlib,
        sasl,
        lager
    ]},
    {mod, {oneprovider_ccm, []}},
    {env, []}
]}.