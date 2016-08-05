%%%--------------------------------------------------------------------
%%% @author Tomasz Lichon
%%% @copyright (C) 2016 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc
%%% Tests of custom metadata manipulation module
%%% @end
%%%--------------------------------------------------------------------
-module(custom_meta_manipulation_test).
-author("Tomasz Lichon").

-include_lib("eunit/include/eunit.hrl").
-include_lib("ctool/include/posix/errors.hrl").

-define(SAMPLE_METADATA_BINARY, <<"
{
  \"name\": \"examplecorp-mymodule\",
  \"version\": 1,
  \"author\": \"Pat\",
  \"license\": \"Apache-2.0\",
  \"summary\": \"A module for a thing\",
  \"source\": \"https://github.com/examplecorp/examplecorp-mymodule\",
  \"project_page\": \"https://forge.puppetlabs.com/examplecorp/mymodule\",
  \"issues_url\": \"https://github.com/examplecorp/examplecorp-mymodule/issues\",
  \"tags\": [\"things\", \"stuff\"],
  \"operatingsystem_support\":
    {
    \"operatingsystem\":\"RedHat\",
    \"operatingsystemrelease\":[ \"5.0\", \"6.0\" ]
    },
  \"dependencies\": [
    { \"name\": \"puppetlabs/stdlib\", \"version_requirement\": \">= 3.2.0 < 5.0.0\" },
    { \"name\": \"puppetlabs/firewall\", \"version_requirement\": \">= 0.0.4 < 2.0.0\" }
  ],
  \"data_provider\": \"hiera\"
}">>).


get_level_0_simple_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    Meta = custom_meta_manipulation:find(Json, [<<"author">>]),
    ?assertEqual(<<"Pat">>, Meta).

get_level_0_integer_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    Meta = custom_meta_manipulation:find(Json, [<<"version">>]),
    ?assertEqual(1, Meta).

get_level_0_list_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    Meta = custom_meta_manipulation:find(Json, [<<"tags">>]),
    ?assertEqual([<<"things">>, <<"stuff">>], Meta).

get_level_0_json_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    Meta = custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>]),
    ?assertMatch(#{<<"operatingsystem">> := <<"RedHat">>, <<"operatingsystemrelease">> := [<<"5.0">>, <<"6.0">>]}, Meta).

get_level_1_simple_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    Meta = custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"operatingsystem">>]),
    ?assertEqual(<<"RedHat">>, Meta).

get_level_1_list_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    Meta = custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>]),
    ?assertEqual([<<"5.0">>, <<"6.0">>], Meta).

get_level_0_all_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    ?assertEqual(Json, custom_meta_manipulation:find(Json, [])).

error_get_level_0_missing_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    ?assertException(throw, {error, ?ENOATTR}, custom_meta_manipulation:find(Json, [<<"missing">>])).

error_get_level_1_missing_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    ?assertException(throw, {error, ?ENOATTR}, custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"missing">>])).

error_get_level_1_meta_from_non_container_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    ?assertException(throw, {error, ?ENOATTR}, custom_meta_manipulation:find(Json, [<<"version">>, <<"missing">>])).

add_level_0_simple_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    NewJson = custom_meta_manipulation:insert(Json, <<"Tom">>, [<<"second_author">>]),
    ?assertEqual(Json#{<<"second_author">> => <<"Tom">>}, NewJson).

add_level_0_json_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    NewJson = custom_meta_manipulation:insert(Json, #{<<"first">> => <<"Tom">>, <<"second">> => <<"Pat">>}, [<<"new_author">>]),
    ?assertEqual(Json#{<<"new_author">> => #{<<"first">> => <<"Tom">>, <<"second">> => <<"Pat">>}}, NewJson).

add_level_0_list_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    NewJson = custom_meta_manipulation:insert(Json, [<<"Tom">>, <<"Pat">>], [<<"new_author">>]),
    ?assertEqual(Json#{<<"new_author">> => [<<"Tom">>, <<"Pat">>]}, NewJson).

override_level_0_list_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    NewJson = custom_meta_manipulation:insert(Json, [<<"Tom">>, <<"Pat">>], [<<"author">>]),
    ?assertEqual(Json#{<<"author">> => [<<"Tom">>, <<"Pat">>]}, NewJson).

add_level_1_json_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    NewJson = custom_meta_manipulation:insert(Json, <<"yum">>, [<<"operatingsystem_support">>, <<"repo_type">>]),
    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"repo_type">> => <<"yum">>,
        <<"operatingsystem">> => <<"RedHat">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"6.0">>]}}, NewJson).

override_level_1_json_meta_test() ->
    Json = jiffy:decode(?SAMPLE_METADATA_BINARY, [return_maps]),
    NewJson = custom_meta_manipulation:insert(Json, <<"Ubuntu">>, [<<"operatingsystem_support">>, <<"operatingsystem">>]),
    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"operatingsystem">> => <<"Ubuntu">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"6.0">>]}}, NewJson).