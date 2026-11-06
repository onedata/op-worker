%%%-------------------------------------------------------------------
%%% @author Jakub Kudzia
%%% @copyright (C) 2018 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% Macros and records used by file_popularity_view module
%%% @end
%%%-------------------------------------------------------------------

-ifndef(FILE_POPULARITY_VIEW_HRL).
-define(FILE_POPULARITY_VIEW_HRL, 1).

-define(FILE_POPULARITY_VIEW(SpaceId), <<"file-popularity-", SpaceId/binary>>).

-endif.