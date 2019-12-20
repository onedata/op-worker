%%%-------------------------------------------------------------------
%%% @author Bartosz Walkowicz
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% Common definitions used in initializer and test suites.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(INITIALIZER_HRL).
-define(INITIALIZER_HRL, 1).

-define(DUMMY_PROVIDER_IDENTITY_TOKEN(__ProviderId), <<"DUMMY-PROVIDER-IDENTITY-TOKEN-", __ProviderId/binary>>).
-define(DUMMY_PROVIDER_ACCESS_TOKEN(__ProviderId), <<"DUMMY-PROVIDER-ACCESS-TOKEN-", __ProviderId/binary>>).

-endif.
