import argparse


def get_args():
    parser = argparse.ArgumentParser("Sentiment-calssfication")
    parser.add_argument(
        '--train_path',
        type=str,
        default='./dataset/train.txt',
        required=False,
        help='train datasets  path')
    parser.add_argument(
        '--test_path',
        type=str,
        default='./dataset/test.txt',
        required=False,
        help='test datasets  path')
    parser.add_argument(
        '--validation_path',
        type=str,
        default='./dataset/validation.txt',
        required=False,
        help='validation datasets  path')
    parser.add_argument(
        '--pred_word2vec_path',
        type=str,
        default='./dataset/wiki_word2vec_50.bin',
        required=False,
        help='pretrained word2vec path')
    parser.add_argument(
        '--tensorboard_path',
        type=str,
        default='./tensorboard',
        required=False,
        help='tensorboard file save path')
    parser.add_argument(
        '--model_save_path',
        type=str,
        default='./modelDict/model.pth',
        required=False,
        help='model save path')
    parser.add_argument('--seed', type=int, default=0, help='random seed')
    parser.add_argument(
        '--embedding_dim',
        type=int,
        default=50,
        help='embedding dimension 50 to fit pretrain word2vec')
    parser.add_argument(
        '--hidden_dim',
        type=int,
        default=100,
        help='lstm layer hidden state dimension')
    parser.add_argument(
        '--batch_size',
        type=int,
        default=8,
        help='batch size')
    parser.add_argument(
        '--LSTM_layers',
        type=int,
        default=3,
        help='layers num of LSTM')
    parser.add_argument(
        '--drop_prob',
        type=float,
        default=0.5,
        help='dropout probability')
    parser.add_argument('--epochs', type=int, default=30, help='batch size')
    parser.add_argument(
        '--lr',
        type=float,
        default=0.001,
        help='initial learning rate')
    # parser.add_argument('--comment_str', type=str, default='电影不错',
    #                     required=False, help='comment string ')
    args = parser.parse_args()
    return args