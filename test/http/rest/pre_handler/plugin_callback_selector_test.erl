%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%% @end
%%%--------------------------------------------------------------------
%%% @doc Unit tests for plugin_callback_selector module.
%%%--------------------------------------------------------------------
-module(plugin_callback_selector_test).
-author("Michal Cwiertnia").

-include_lib("eunit/include/eunit.hrl").

-define(CT_APP_ANY_CALLBACK, app_any_callback).
-define(CT_APP_JSON_CALLBACK, app_json_callback).
-define(CT_IMAGE_ANY_CALLBACK, image_any_callback).
-define(CT_IMAGE_PNG_CALLBACK, image_png_callback).

-define(REQ, #{}).

-define(ANY_MIME_TYPE, {<<"*">>, <<"*">>, []}).
-define(APP_JSON_MIME_TYPE, {<<"application">>, <<"json">>, []}).
-define(APP_ANY_MIME_TYPE, {<<"application">>, <<"*">>, []}).
-define(IMAGE_ANY_MIME_TYPE, {<<"image">>, <<"*">>, []}).
-define(IMAGE_PNG_MIME_TYPE, {<<"image">>, <<"png">>, []}).
-define(TEXT_ANY_MIME_TYPE, {<<"text">>, <<"*">>, []}).

-define(ACCEPT_ANY, {?ANY_MIME_TYPE, 1000, []}).
-define(ACCEPT_APP_ANY, {?APP_ANY_MIME_TYPE, 1000, []}).
-define(ACCEPT_APP_JSON, {?APP_JSON_MIME_TYPE, 1000, []}).
-define(ACCEPT_IMAGE_ANY, {?IMAGE_ANY_MIME_TYPE, 1000, []}).
-define(ACCEPT_IMAGE_PNG, {?IMAGE_PNG_MIME_TYPE, 1000, []}).
-define(ACCEPT_TEXT_ANY, {?TEXT_ANY_MIME_TYPE, 1000, []}).

-define(CONTENT_TYPE_APP_ANY, {?APP_ANY_MIME_TYPE, ?CT_APP_ANY_CALLBACK}).
-define(CONTENT_TYPE_APP_JSON, {?APP_JSON_MIME_TYPE, ?CT_APP_JSON_CALLBACK}).
-define(CONTENT_TYPE_IMAGE_ANY, {?IMAGE_ANY_MIME_TYPE, ?CT_IMAGE_ANY_CALLBACK}).
-define(CONTENT_TYPE_IMAGE_PNG, {?IMAGE_PNG_MIME_TYPE, ?CT_IMAGE_PNG_CALLBACK}).


empty_content_type_should_always_return_undefined_callback_test_() ->
  [
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [], [{}]
      )
    ),
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [], [?ANY_MIME_TYPE]
      )
    ),
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [], [?APP_JSON_MIME_TYPE]
      )
    )
  ].

accept_any_header_should_return_appropriate_callback_test_() ->
  [
    ?_assertMatch(?CT_APP_ANY_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_ANY], [?ACCEPT_ANY]
      )
    ),
    ?_assertMatch(?CT_APP_JSON_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_JSON], [?ACCEPT_ANY]
      )
    )
  ].

accept_and_content_type_with_the_same_subtype_and_type_should_return_appropriate_callback_test_() ->
  [
    ?_assertMatch(?CT_APP_JSON_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_JSON], [?ACCEPT_APP_JSON]
      )
    ),
    ?_assertMatch(?CT_IMAGE_PNG_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_IMAGE_PNG], [?ACCEPT_IMAGE_PNG]
      )
    )
  ].

accept_and_content_type_with_the_same_type_and_accept_subtype_any_should_return_appropriate_callback_test_() ->
  [
    ?_assertMatch(?CT_APP_JSON_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_JSON], [?ACCEPT_APP_ANY]
      )
    ),
    ?_assertMatch(?CT_APP_ANY_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_ANY], [?ACCEPT_APP_ANY]
      )
    ),
    ?_assertMatch(?CT_IMAGE_PNG_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_IMAGE_PNG], [?ACCEPT_IMAGE_ANY]
      )
    ),
    ?_assertMatch(?CT_IMAGE_ANY_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_IMAGE_ANY], [?ACCEPT_IMAGE_ANY]
      )
    )
  ].

accept_with_any_subtype_but_type_different_than_content_type_should_return_undefined_callback_test_() ->
  [
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_JSON], [?ACCEPT_IMAGE_ANY]
      )
    ),
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_ANY], [?ACCEPT_IMAGE_ANY]
      )
    ),
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_IMAGE_PNG], [?ACCEPT_APP_ANY]
      )
    ),
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_IMAGE_ANY], [?ACCEPT_APP_ANY]
      )
    )
  ].

accept_and_content_type_for_which_types_differ_should_return_undefined_callback_test_() ->
  [
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_APP_ANY], [?ACCEPT_APP_JSON]
      )
    ),
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, [?CONTENT_TYPE_IMAGE_ANY], [?ACCEPT_APP_JSON]
      )
    )
  ].

content_type_headers_should_be_searched_recursively_test_() ->
  ContentTypesList = [?CONTENT_TYPE_APP_JSON, ?CONTENT_TYPE_APP_ANY,
    ?CONTENT_TYPE_IMAGE_PNG],
  [
    ?_assertMatch(?CT_IMAGE_PNG_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, ContentTypesList, [?ACCEPT_IMAGE_PNG]
      )
    ),
    ?_assertMatch(?CT_APP_JSON_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, ContentTypesList, [?ACCEPT_APP_JSON]
      )
    ),
    ?_assertMatch(?CT_APP_JSON_CALLBACK,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, ContentTypesList, [?ACCEPT_APP_ANY]
      )
    ),
    ?_assertMatch(undefined,
      plugin_callback_selector:choose_provide_content_type_callback(
        ?REQ, ContentTypesList, [?ACCEPT_TEXT_ANY]
      )
    )
  ].
