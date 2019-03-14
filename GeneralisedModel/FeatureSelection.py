class FeatureSelection:
    def feature_to_select(features, pair):
        pairs = pair.split('|')
        subset = features[pairs]
        subset['delay'] = features['delay']
        crmat = subset.corr()
        first_corr = crmat[pairs[0]]['delay']
        second_corr = crmat[pairs[1]]['delay']
        if first_corr < second_corr:
            return pairs[1]
        else:
            return pairs[0]

    def eliminate_correlation(features, threshold=.8):
        x_features = features.drop('delay', axis=1)
        corrMatrix = x_features.corr()
        feature_to_select_pair = []
        optimal_feature_set = []
        optimal_feature_set.append('delay')
        q = corrMatrix.shape[0]
        for i in range(q - 1):
            currMax = 0
            currPos = 0
            flag = False
            for j in range(i + 1, q):
                if currMax < corrMatrix[corrMatrix.keys()[i]][corrMatrix.keys()[j]]:
                    currMax = corrMatrix[corrMatrix.keys()[i]][corrMatrix.keys()[j]]
                    if (corrMatrix[corrMatrix.keys()[i]][corrMatrix.keys()[j]] >= threshold):
                        flag = True
                    currPos = corrMatrix.keys()[j] + "|" + corrMatrix.keys()[i]
            if flag == True:
                feature_to_select_pair.append(currPos)
            else:
                if corrMatrix.keys()[i] not in (optimal_feature_set):
                    optimal_feature_set.append(corrMatrix.keys()[i])
        for pair in feature_to_select_pair:
            feat = feature_to_select(features, pair)
            if feat not in (optimal_feature_set):
                optimal_feature_set.append(feat)
        return optimal_feature_set
