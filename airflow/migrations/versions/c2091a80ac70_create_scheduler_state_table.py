#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""create scheduler_state table

Revision ID: c2091a80ac70
Revises: d2ae31099d61
Create Date: 2019-11-14 16:22:10.159454

"""

# revision identifiers, used by Alembic.
revision = 'c2091a80ac70'
down_revision = 'd2ae31099d61'
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa


def upgrade():
    op.create_table(
        'scheduler_state',
        sa.Column('state', sa.String(50), primary_key=True)
    )

def downgrade():
    op.drop_table("scheduler_state")
