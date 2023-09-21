import pandas as pd
import pkg_resources
import unittest
from stitches.fx_util import check_columns
from stitches.fx_recipe import get_num_perms, remove_duplicates, permute_stitching_recipes, make_recipe
from stitches.fx_match import match_neighborhood

class TestRecipe(unittest.TestCase):
    # ###################################################
    # some test data
    # ###################################################
    # real ESM data that know will have some duplicates, etc to test with.
    # Easier than trying to make complete
    TARGET_DATA = pd.DataFrame(data = {'ensemble':['r1i1p1f1'] * 28,
                                       'variable':['tas'] * 28,
                                       'model':['test_model'] * 28,
                                       'experiment':['ssp245'] * 28,
                                       'start_yr':[1850, 1859, 1868, 1877, 1886, 1895,
                                                   1904, 1913, 1922, 1931, 1940, 1949,
                                                   1958, 1967, 1976, 1985, 1994, 2003,
                                                   2012, 2021, 2030, 2039,  2048, 2057,
                                                   2066, 2075, 2084, 2093],
                                       'end_yr':[1858, 1867, 1876, 1885, 1894, 1903,
                                                  1912, 1921, 1930, 1939, 1948, 1957,
                                                  1966, 1975, 1984, 1993, 2002, 2011,
                                                  2020, 2029, 2038, 2047, 2056, 2065,
                                                  2074, 2083, 2092, 2100],
                                       'year':[1854, 1863, 1872, 1881, 1890, 1899,
                                               1908, 1917, 1926, 1935, 1944, 1953, 1962,
                                               1971, 1980, 1989, 1998, 2007, 2016, 2025,
                                               2034, 2043,2052, 2061, 2070, 2079, 2088,
                                               2097],
                                       'fx':[-1.28986999, -1.26877615, -1.2150827,
                                             -1.19510465,  -1.2504297,   -1.12476316,
                                             -1.18010115, -1.19607191,  -1.05674376,
                                             -1.06501778,  -0.94348729,  -0.95177229,
                                             -0.94919908,  -0.95949291,  -0.69856503,
                                             -0.56833508,  -0.21722963, 0.05139228,
                                             0.40485701,  0.90463641, 1.17029258,
                                             1.51889658,1.83794095, 2.22997751,
                                             2.45954622, 2.62914668, 2.9379544,
                                             3.03012582],
                                       'dx':[-0.00336812, 0.00931957, 0.00475767,
                                             -0.01462664, 0.01469504, -0.00829463,
                                             0.00480449, 0.01124357, 0.00170745,
                                             0.01226593, 0.01214147, -0.01130722,
                                             -0.00533205, 0.0263663, 0.01746445,
                                             0.0110813, 0.05199482, 0.03731981,
                                             0.03515222, 0.04676322, 0.03548417,
                                             0.03356049, 0.04786934, 0.02252523,
                                             0.03451772, 0.0222264, 0.01888666,
                                             0.01404303]})

    ARCHIVE_DATA=pd.DataFrame(data = {'ensemble':(['r2i1p1f1']*28+ ['r3i1p1f1']*28),
                                      'variable': ['tas'] * 56,
                                      'model': ['test_model'] * 56,
                                      'experiment': ['ssp245'] * 56,
                                      'start_yr': [1850, 1859, 1868, 1877, 1886, 1895,
                                                   1904, 1913, 1922, 1931, 1940, 1949,
                                                   1958, 1967, 1976, 1985, 1994, 2003,
                                                   2012, 2021, 2030, 2039, 2048, 2057,
                                                   2066, 2075, 2084, 2093] *2,
                                      'end_yr': [1858, 1867, 1876, 1885, 1894, 1903,
                                                 1912, 1921, 1930, 1939, 1948, 1957,
                                                 1966, 1975, 1984, 1993, 2002, 2011,
                                                 2020, 2029, 2038, 2047, 2056, 2065,
                                                 2074, 2083, 2092, 2100] *2,
                                      'year': [1854, 1863, 1872, 1881, 1890, 1899,
                                               1908, 1917, 1926, 1935, 1944, 1953, 1962,
                                               1971, 1980, 1989, 1998, 2007, 2016, 2025,
                                               2034, 2043, 2052, 2061, 2070, 2079, 2088,
                                               2097]*2,
                                      'fx':[-1.09653809, -1.13315784, -1.0943591, -1.17177001,
                                            -1.32738625, -1.18827856, -1.19859187, -1.17866016,
                                            -1.01826316, -1.00823875, -0.86747065, -0.75741926,
                                            -0.95912482, -0.84078494, -0.70828715,  -0.55041327,
                                            -0.28319994,  0.09212478,  0.60853009,  1.00862082,
                                            1.3594721,  1.78422761,  2.01833223,  2.34533018,
                                            2.65273186,2.96080091,  2.96904659,  3.0398682,
                                            -1.19284299, -1.23273612, -1.11309838, -1.20154639,
                                            -1.34276733, -1.28168792, -1.33553245, -1.20231452,
                                            -1.02608287, -0.94425834, -0.92477871, -0.8000345,
                                            -0.92952369, -0.97309169, -0.81131182, -0.61165323,
                                            -0.26776859, 0.0471447,  0.5355102,  0.78498746,
                                            1.2937814,  1.65184098,  2.0555254,  2.38665088,
                                            2.59874084,  2.87927608,  3.04260422, 3.1910005],
                                      'dx':[ 0.01181598, -0.00976008,  0.00192164, -0.0302527,
                                             0.01037644, 0.00079727,  .01406466,  0.00724873,
                                             0.00108201,  0.02716604, -0.00360448,  0.00981227,
                                             -0.03041208,  0.02847502,  0.01515118, 0.01836129,
                                             0.05547686,  0.03248089,  0.06159063,  0.03715717,
                                             0.04961695,  0.0277128,  0.03308734,  0.04132718,
                                             0.02824231, 0.01661322,  0.00860536, -0.0009309,
                                             -0.00881434,  0.0083732, 0.00478882, -0.02305601,
                                             0.01004446, -0.00554876,  0.00565863, 0.01500687,
                                             0.01383536,  0.01050035,  0.00444087,  0.00508502,
                                             -0.02231988,  0.01092053,  0.0227568,  0.02147527,
                                             0.04722855,  0.0478629,  0.0360716,  0.04215114,
                                             0.04714541,  0.04151509, 0.04368013,  0.02965839,
                                             0.03212568,  0.02365025,  0.01311625,  0.01300568]
                                      })


    # Data relevant for remove_duplicates()
    # these are the duplicate matches that should result from match_neighborhood
    # with tol=0 on the above data sets.
    DUPLICATES=pd.DataFrame(data={'target_variable':['tas']*4,
                                  'target_experiment':['ssp245']*4,
                                  'target_ensemble':['r1i1p1f1'] * 4,
                                  'target_model':['test_model'] * 4,
                                  'target_start_yr':[1859, 1868, 2057, 2066],
                                  'target_end_yr':[1867, 1876, 2065, 2074],
                                  'target_year':[1863, 1872, 2061, 2070],
                                  'target_fx':[-1.26877615, -1.2150827 ,  2.22997751,  2.45954622],
                                  'target_dx':[0.00931957, 0.00475767, 0.02252523, 0.03451772],
                                  'archive_experiment':['ssp245']*4,
                                  'archive_variable':['tas']*4,
                                  'archive_model':['test_model'] * 4,
                                  'archive_ensemble':['r3i1p1f1', 'r3i1p1f1', 'r3i1p1f1', 'r3i1p1f1'],
                                  'archive_start_yr':[1859, 1859, 2057, 2057],
                                  'archive_end_yr':[1867, 1867, 2065, 2065],
                                  'archive_year':[1863, 1863, 2061, 2061],
                                  'archive_fx':[-1.23273612, -1.23273612,  2.38665088,  2.38665088],
                                  'archive_dx':[0.0083732 , 0.0083732 , 0.02965839, 0.02965839],
                                  'dist_dx':[0.00757096, 0.02892424, 0.05706528, 0.03887464],
                                  'dist_fx':[0.03604003, 0.01765342, 0.15667337, 0.07289534],
                                  'dist_l2':[0.03682666, 0.03388591, 0.16674229, 0.08261337]})

    # Target years r1-1863 and r1-1872 both match to the same archive point: r3-1863.
    # dist_l2 for r1-1872 = 0.034
    # dist_l2 for r1-1863 = 0.037
    # So r1-1872 keeps the match r3-1863 and r1-1863 needs to be rematched
    #
    # similarly targets r1-2061 and r1-2070 match to the same archive point: r3-2061.
    # dist_l2 for r1-2061 = 0.17
    # dist_l2 for r1-2070 = 0.08
    # So r1-2070 keeps the match and r1-2061 needs to be rematched.
    #
    # So we will explicitly check that remove_dupilcates() does this.
    REMATCHED_YEARS =[1863, 2061]

    NEW_MATCHES = pd.DataFrame(data={'target_variable': ['tas']*2,
                                     'target_experiment':['ssp245'] *2,
                                     'target_ensemble':['r1i1p1f1'] *2,
                                     'target_model':['test_model']*2,
                                     'target_start_yr':[1859, 2057],
                                     'target_end_yr':[1867, 2065],
                                     'target_year':[1863, 2061],
                                     'target_fx':[-1.26877615,  2.22997751],
                                     'target_dx':[0.00931957, 0.02252523],
                                     'archive_experiment':['ssp245'] *2,
                                     'archive_variable': ['tas']*2,
                                     'archive_model':['test_model']*2,
                                     'archive_ensemble': ['r2i1p1f1'] *2,
                                     'archive_start_yr':[1886,2057],
                                     'archive_end_yr':[1894,2065],
                                     'archive_year':[1890,2061],
                                     'archive_fx':[-1.32738625,  2.34533018],
                                     'archive_dx':[0.01037644, 0.04132718],
                                     'dist_dx':[0.00845496, 0.1504156 ],
                                     'dist_fx':[0.0586101 , 0.11535267],
                                     'dist_l2':[0.05921681, 0.18955498] })

    # ###################################################
    # tests
    # ###################################################

    def test_get_num_perms(self):

        # Read in the match test data.
        path = pkg_resources.resource_filename('stitches', 'tests/test-match_w_dup.csv')
        match_data = pd.read_csv(path)

        "Test get_num_perms"
        out = get_num_perms(match_data)
        self.assertEqual(type(out), list)
        # Throw an error if the output does not match what we would expect.
        check_columns(out[0], set(['target_variable', 'target_experiment', 'target_ensemble',
                                   'target_model', 'minNumMatches', 'totalNumPerms']))
        check_columns(out[1], set(['target_variable', 'target_experiment', 'target_ensemble',
                                   'target_model', 'target_start_yr', 'target_end_yr', 'target_year',
                                   'target_fx', 'target_dx', 'n_matches']))
        self.assertEqual(len(out), 2, "Test get_num_perms")


    def test_remove_duplicates(self):
        """Test to make sure the remove_duplicates function if working correctly."""

        # Initial match data
        md = match_neighborhood(TestRecipe.TARGET_DATA,
                                TestRecipe.ARCHIVE_DATA,
                                tol=0.0)

        # # #####################################################
        # # Check to see if in the matched data frame if there are any repeated values.
        # md_archive = md[['archive_experiment', 'archive_variable', 'archive_model',
        #                            'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
        #                            'archive_year', 'archive_fx', 'archive_dx']].copy()
        # duplicates = md.merge(md_archive[md_archive.duplicated()], how="inner").copy()
        #
        # # assert that the correct duplicate matches returned from match_neighborhood()
        # pd.testing.assert_frame_equal(duplicates, TestRecipe.DUPLICATES)
        #
        # # #####################################################

        cleaned_matched_data = remove_duplicates(md=md, archive=TestRecipe.ARCHIVE_DATA)

        # every target point but r1-1863 and r1-2061 should be identical to md
        pd.testing.assert_frame_equal(md[-md['target_year'].isin(TestRecipe.REMATCHED_YEARS)],
                                        cleaned_matched_data[-cleaned_matched_data['target_year'].isin(TestRecipe.REMATCHED_YEARS)])

        # and r1-1863 and r1-2061 should have been rematched with the resulting
        # matches described in TestRecipe.NEW_MATCHES
        pd.testing.assert_frame_equal(cleaned_matched_data[cleaned_matched_data['target_year'].isin(TestRecipe.REMATCHED_YEARS)].reset_index(drop=True),
                                      TestRecipe.NEW_MATCHES)


    def test_permute_stitching_recipes(self):
        # With tol < 0.17, the test data can only support one collapse free
        # recipe. So a message will be printed to that effect
        messy_rp1 = permute_stitching_recipes(N_matches=2,
                                             matched_data=match_neighborhood(TestRecipe.TARGET_DATA,
                                                                             TestRecipe.ARCHIVE_DATA,
                                                                             tol=0.07),
                                             archive=TestRecipe.ARCHIVE_DATA,
                                             testing=True)


        # Test that the recipe has the same number of time windows as the
        # target:
        self.assertEqual(len(messy_rp1), len(TestRecipe.TARGET_DATA),
                         'drawn recipe does not have the same number of time windows as target ')


        # Test that all of the recipe windows did in fact come from the archive:
        cols = [col for col in messy_rp1 if col.startswith('archive_')]
        archive_check = messy_rp1[cols]
        new_names = list(map(lambda x: x.replace('archive_', ''), archive_check.columns))
        archive_check.columns = new_names
        self.assertTrue(len(archive_check.merge(TestRecipe.ARCHIVE_DATA)) == len(archive_check),
                        'recipe has entries not from the archive somehow.')


        # Test that no two target points got matched to the same archive point
        # (ie that remove_duplicates was called correctly in permute_stitching_recipes)
        md_archive = messy_rp1[['archive_experiment', 'archive_variable', 'archive_model',
                                   'archive_ensemble', 'archive_start_yr', 'archive_end_yr',
                                   'archive_year', 'archive_fx', 'archive_dx']]
        duplicates = messy_rp1.merge(md_archive[md_archive.duplicated()], how="inner")
        self.assertEqual(len(duplicates), 0,
                         'issue with call to remove_duplicates() in permute_stitching_recipe().')

        # Test that our reproducible mode (testing=True) does produce the
        # same recipe:
        messy_rp2 = permute_stitching_recipes(N_matches=2,
                                              matched_data=match_neighborhood(TestRecipe.TARGET_DATA,
                                                                              TestRecipe.ARCHIVE_DATA,
                                                                              tol=0.07),
                                              archive=TestRecipe.ARCHIVE_DATA,
                                              testing=True)
        self.assertTrue(messy_rp2.equals(messy_rp1),
                        'reproducible mode of permute_stitching_recipe failing 1')

        # Test that turning it off produces something different:
        messy_rp3 = permute_stitching_recipes(N_matches=2,
                                              matched_data=match_neighborhood(TestRecipe.TARGET_DATA,
                                                                              TestRecipe.ARCHIVE_DATA,
                                                                              tol=0.07),
                                              archive=TestRecipe.ARCHIVE_DATA,
                                              testing=False)
        self.assertTrue(not messy_rp3.equals(messy_rp1),
                        'reproducible mode of permute_stitching_recipe failing 2')

        # Test that we don't have collapse when targeting multiple
        # ensemble members.
        # combine the TARGET and ARCHIVE data sets so we have more ensemble
        # members to work with:
        target2 = pd.concat([TestRecipe.ARCHIVE_DATA, TestRecipe.TARGET_DATA]).reset_index(drop=True).copy()
        archive2 = pd.concat([TestRecipe.ARCHIVE_DATA, TestRecipe.TARGET_DATA]).reset_index(drop=True).copy()

        pd.set_option('display.max_columns', None)

        messy_rp = permute_stitching_recipes(N_matches=2,
                                              matched_data=match_neighborhood(target2,
                                                                              archive2,
                                                                              tol=0.2),
                                              archive=archive2,
                                              testing=True)
        print(messy_rp)

        # Test that there is no envelope collapse across ensemble members.
        # We define collapse across ensemble members as 'you don't have
        # realization 1 and realization 4 2070 getting matched to the same archive point.'
        # if r1-2050 and r4-2070 get matched to the same archive point,
        # that's fine. It's just multiple target  realizations having
        cols = [col for col in messy_rp if col.startswith('archive_')]

        for year in messy_rp['target_year'].unique():
            tmp = messy_rp[messy_rp['target_year']==year].copy()
            unique_archive_rows = tmp[cols].drop_duplicates().copy()
            print(tmp)
            print(unique_archive_rows)
            self.assertEqual(len(tmp), len(unique_archive_rows),
                             'Envelope collapse has occurred')
            del(tmp)
            del(unique_archive_rows)




if __name__ == '__main__':
    unittest.main()
