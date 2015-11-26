%%%--------------------------------------------------------------------
%%% @author Piotr Ociepka
%%% @copyright (C) 2015 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Unit tests for cdmi_arg_parser module.
%%% @end
%%%--------------------------------------------------------------------
-module(cdmi_arg_parser_test).
-author("Piotr Ociepka").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("modules/http_worker/rest/cdmi/cdmi_errors.hrl").

-define(version, cdmi_arg_parser:get_supported_version).


get_supported_version_test() ->
  Binary00 = <<"">>,
  Binary01 = <<"  ">>,
  Binary02 = <<" ,">>,
  Binary10 = <<"1.1.1">>,
  Binary11 = <<"aaa,1.1.1 ,aaa">>,
  Binary20 = <<"1.0.2">>,
  Binary21 = <<"   2.3.4   ">>,
  Binary22 = <<",42.0.0,">>,
  Binary23 = <<"bbb,1.0.1,ccc">>,
  Binary30 = <<"1">>,
  Binary31 = <<"1.">>,
  Binary32 = <<"1.2">>,
  Binary33 = <<"1.2.">>,
  Binary34 = <<"1.2.3.">>,
  Binary35 = <<"1.2.3.4">>,
  Binary40 = <<"aaaaa">>,
  Binary41 = <<"aaa,aa,">>,
  Binary42 = <<"aaa,aa ">>,

  ?assertThrow(?unsupported_version, ?version(Binary00)),
  ?assertThrow(?unsupported_version, ?version(Binary01)),
  ?assertThrow(?unsupported_version, ?version(Binary02)),

  ?assertEqual(<<"1.1.1">>, ?version(Binary10)),
  ?assertEqual(<<"1.1.1">>, ?version(Binary11)),

  ?assertThrow(?unsupported_version, ?version(Binary20)),
  ?assertThrow(?unsupported_version, ?version(Binary21)),
  ?assertThrow(?unsupported_version, ?version(Binary22)),
  ?assertThrow(?unsupported_version, ?version(Binary23)),


  ?assertThrow(?unsupported_version, ?version(Binary30)),
  ?assertThrow(?unsupported_version, ?version(Binary31)),
  ?assertThrow(?unsupported_version, ?version(Binary32)),
  ?assertThrow(?unsupported_version, ?version(Binary33)),
  ?assertThrow(?unsupported_version, ?version(Binary34)),
  ?assertThrow(?unsupported_version, ?version(Binary35)),

  ?assertThrow(?unsupported_version, ?version(Binary40)),
  ?assertThrow(?unsupported_version, ?version(Binary41)),
  ?assertThrow(?unsupported_version, ?version(Binary42)).

-endif.