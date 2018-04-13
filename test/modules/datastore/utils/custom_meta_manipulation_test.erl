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
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    Meta = custom_meta_manipulation:find(Json, [<<"author">>]),
    ?assertEqual(<<"Pat">>, Meta).

get_level_0_integer_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    Meta = custom_meta_manipulation:find(Json, [<<"version">>]),
    ?assertEqual(1, Meta).

get_level_0_list_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    Meta = custom_meta_manipulation:find(Json, [<<"tags">>]),
    ?assertEqual([<<"things">>, <<"stuff">>], Meta).

get_level_0_json_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    Meta = custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>]),
    ?assertMatch(#{<<"operatingsystem">> := <<"RedHat">>, <<"operatingsystemrelease">> := [<<"5.0">>, <<"6.0">>]}, Meta).

get_level_1_simple_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    Meta = custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"operatingsystem">>]),
    ?assertEqual(<<"RedHat">>, Meta).

get_level_1_list_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    Meta = custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>]),
    ?assertEqual([<<"5.0">>, <<"6.0">>], Meta).

get_level_0_all_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    ?assertEqual(Json, custom_meta_manipulation:find(Json, [])).

error_get_level_0_missing_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    ?assertException(throw, {error, ?ENOATTR}, custom_meta_manipulation:find(Json, [<<"missing">>])).

error_get_level_1_missing_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    ?assertException(throw, {error, ?ENOATTR}, custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"missing">>])).

error_get_level_1_meta_from_non_container_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    ?assertException(throw, {error, ?ENOATTR}, custom_meta_manipulation:find(Json, [<<"version">>, <<"missing">>])).

add_level_0_simple_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"Tom">>, [<<"second_author">>]),
    ?assertEqual(Json#{<<"second_author">> => <<"Tom">>}, NewJson).

add_level_0_json_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, #{<<"first">> => <<"Tom">>, <<"second">> => <<"Pat">>}, [<<"new_author">>]),
    ?assertEqual(Json#{<<"new_author">> => #{<<"first">> => <<"Tom">>, <<"second">> => <<"Pat">>}}, NewJson).

add_level_0_list_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, [<<"Tom">>, <<"Pat">>], [<<"new_author">>]),
    ?assertEqual(Json#{<<"new_author">> => [<<"Tom">>, <<"Pat">>]}, NewJson).

override_level_0_list_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, [<<"Tom">>, <<"Pat">>], [<<"author">>]),
    ?assertEqual(Json#{<<"author">> => [<<"Tom">>, <<"Pat">>]}, NewJson).

add_level_1_json_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"yum">>, [<<"operatingsystem_support">>, <<"repo_type">>]),
    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"repo_type">> => <<"yum">>,
        <<"operatingsystem">> => <<"RedHat">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"6.0">>]}}, NewJson).

override_level_1_json_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"Ubuntu">>, [<<"operatingsystem_support">>, <<"operatingsystem">>]),
    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"operatingsystem">> => <<"Ubuntu">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"6.0">>]}}, NewJson).

merge_single_json_test() ->
    Json1 = #{<<"a">> => <<"b">>},

    NewJson = custom_meta_manipulation:merge([Json1]),

    ?assertEqual(#{<<"a">> => <<"b">>}, NewJson).

merge_empty_jsons_test() ->
    Json1 = #{},
    Json2 = #{},

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(#{}, NewJson).

merge_empty_json_and_primitive_test() ->
    Json1 = #{},
    Json2 = <<"string">>,

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(#{}, NewJson).

merge_primitive_and_empty_json_test() ->
    Json1 = <<"string">>,
    Json2 = #{},

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(<<"string">>, NewJson).

merge_empty_json_and_non_empty_json_test() ->
    Json1 = #{},
    Json2 = #{<<"a">> => <<"b">>, <<"c">> => []},

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(#{<<"a">> => <<"b">>, <<"c">> => []}, NewJson).

merge_non_empty_json_and_empty_json_test() ->
    Json1 = #{<<"a">> => <<"b">>, <<"c">> => []},
    Json2 = #{},

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(#{<<"a">> => <<"b">>, <<"c">> => []}, NewJson).

merge_jsons_without_common_keys_test() ->
    Json1 = #{<<"a">> => <<"b">>, <<"c">> => #{<<"d">> => <<"d">>}},
    Json2 = #{<<"e">> => <<"f">>, <<"g">> => []},

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(#{<<"a">> => <<"b">>, <<"c">> => #{<<"d">> => <<"d">>}, <<"e">> => <<"f">>, <<"g">> => []}, NewJson).

merge_jsons_with_simple_common_key_test() ->
    Json1 = #{<<"a">> => <<"b">>, <<"c">> => <<"d">>},
    Json2 = #{<<"a">> => <<"f">>, <<"g">> => []},

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(#{<<"a">> => <<"b">>, <<"c">> => <<"d">>, <<"g">> => []}, NewJson).

merge_jsons_with_json_as_common_key_test() ->
    Json1 = #{<<"a">> => #{<<"a1">> => <<"b1">>}},
    Json2 = #{<<"a">> => #{<<"a2">> => <<"b2">>}},

    NewJson = custom_meta_manipulation:merge([Json1, Json2]),

    ?assertEqual(#{<<"a">> =>  #{<<"a1">> => <<"b1">>, <<"a2">> => <<"b2">>}}, NewJson).

merge_three_jsons_test() ->
    Json1 = #{<<"a">> => #{<<"a1">> => <<"b1">>}},
    Json2 = #{<<"a">> => #{<<"a2">> => <<"b2">>}, <<"b">> => <<"c">>},
    Json3 = #{<<"a">> => #{<<"a1">> => <<"b4">>, <<"a3">> => <<"b3">>}, <<"d">> => <<"e">>},

    NewJson = custom_meta_manipulation:merge([Json1, Json2, Json3]),

    ?assertEqual(#{<<"a">> =>  #{<<"a1">> => <<"b1">>, <<"a2">> => <<"b2">>, <<"a3">> => <<"b3">>}, <<"b">> => <<"c">>, <<"d">> => <<"e">>}, NewJson).

override_list_meta_begin_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"4.0">>, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[0]">>]),

    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"operatingsystem">> => <<"RedHat">>,
        <<"operatingsystemrelease">> => [<<"4.0">>, <<"6.0">>]}}, NewJson).

override_list_meta_end_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"7.0">>, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[1]">>]),
    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"operatingsystem">> => <<"RedHat">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"7.0">>]}}, NewJson).

append_to_list_meta_end_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"7.0">>, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[2]">>]),
    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"operatingsystem">> => <<"RedHat">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"6.0">>, <<"7.0">>]}}, NewJson).

create_gap_in_list_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"7.0">>, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[4]">>]),
    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"operatingsystem">> => <<"RedHat">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"6.0">>, null, null, <<"7.0">>]}}, NewJson).

create_list_in_list_meta_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    NewJson = custom_meta_manipulation:insert(Json, <<"7.1">>, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[2]">>, <<"[0]">>]),

    ?assertEqual(Json#{<<"operatingsystem_support">> => #{
        <<"operatingsystem">> => <<"RedHat">>,
        <<"operatingsystemrelease">> => [<<"5.0">>, <<"6.0">>, [<<"7.1">>]]}}, NewJson).

get_list_meta_begin_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    ?assertEqual(<<"5.0">>, custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[0]">>])).

get_list_meta_end_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    ?assertEqual(<<"6.0">>, custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[1]">>])).

get_list_meta_missing_test() ->
    Json = json_utils:decode(?SAMPLE_METADATA_BINARY),
    ?assertException(throw, {error, ?ENOATTR}, custom_meta_manipulation:find(Json, [<<"operatingsystem_support">>, <<"operatingsystemrelease">>, <<"[2]">>])).
