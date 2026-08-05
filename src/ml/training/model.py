"""PyTorch sequence model architecture.

Defines a stacked LSTM model for classification tasks, with weights initialized
using industry standards (orthogonal/Xavier).
"""

from __future__ import annotations

import torch
from torch import nn


class CryptoLSTM(nn.Module):
    """Stacked LSTM model for predicting short-horizon price directions."""

    def __init__(
        self,
        input_size: int,
        hidden_size: int = 256,
        num_layers: int = 3,
        dropout: float = 0.3,
    ) -> None:
        """Initialize model architecture layers.

        Args:
            input_size: Number of features per sequence timestep.
            hidden_size: Size of the hidden layer vectors.
            num_layers: Number of stacked LSTM layers.
            dropout: Dropout probability between layers.
        """
        super().__init__()
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.num_layers = num_layers

        # Stacked LSTM layer
        self.lstm = nn.ModuleList()
        # First layer maps input_size → hidden_size
        self.lstm.append(
            nn.LSTM(
                input_size=input_size,
                hidden_size=hidden_size,
                batch_first=True,
            )
        )
        self.dropouts = nn.ModuleList()
        self.dropouts.append(nn.Dropout(dropout))

        # Intermediate layers map hidden_size → hidden_size
        for _ in range(1, num_layers):
            self.lstm.append(
                nn.LSTM(
                    input_size=hidden_size,
                    hidden_size=hidden_size,
                    batch_first=True,
                )
            )
            self.dropouts.append(nn.Dropout(dropout))

        # Classification output head (binary logit)
        self.classifier = nn.Linear(hidden_size, 2)

        self.reset_parameters()

    def reset_parameters(self) -> None:
        """Apply weight initializations (orthogonal for LSTM, Xavier for linear)."""
        for cell in self.lstm:
            # Orthogonal initialization for LSTM recurrent weights
            for name, param in cell.named_parameters():
                if "weight_hh" in name:
                    nn.init.orthogonal_(param.data)
                elif "weight_ih" in name:
                    nn.init.xavier_uniform_(param.data)
                elif "bias" in name:
                    # Initialize forget gate biases to 1.0 (recommended standard)
                    param.data.fill_(0.0)
                    n = param.size(0)
                    param.data[n // 4 : n // 2].fill_(1.0)

        # Xavier initialization for the output layer
        nn.init.xavier_uniform_(self.classifier.weight)
        if self.classifier.bias is not None:
            self.classifier.bias.data.fill_(0.0)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """Forward pass for the model.

        Args:
            x: Input tensor of shape (batch_size, seq_length, input_size).

        Returns:
            Output logits of shape (batch_size, 2).
        """
        # Sequentially pass through LSTM and dropout layers
        out = x
        for lstm_layer, dropout_layer in zip(self.lstm, self.dropouts, strict=False):
            out, _ = lstm_layer(out)
            out = dropout_layer(out)

        # Take the output of the last sequence timestep
        last_timestep = out[:, -1, :]
        logits = self.classifier(last_timestep)
        assert isinstance(logits, torch.Tensor)
        return logits
